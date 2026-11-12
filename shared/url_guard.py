"""Keep the worker from being used to call things inside its own network.

A tenant chooses two kinds of URL: the source an ingestion step downloads, and the webhook that is told when a run
ends. The worker runs inside the cluster, so without a check a tenant could point either at the cloud metadata
service (169.254.169.254, which hands out the node's credentials), at Postgres, at Redis, or at another service that
trusts anything inside the network.

The check is made where the connection is made, not only when the URL is saved. A name is looked up, every address it
gives must be public, and the connection goes to the address that was checked. So a name that answers with a public
address the first time and a private one the second (DNS rebinding) cannot get through, and neither can redirects,
which are not followed anyway.

ALLOW_PRIVATE_URLS=true switches the address check off, for a developer machine or a cluster where every tenant is
trusted. The scheme check stays.
"""
import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

import httpcore
import httpx

from shared import config


class UnsafeURLError(ValueError):
    """The URL is not one the worker may call."""


def check_url_syntax(url):
    """Only http and https, and a host. No network access. Returns the URL, for use as a validator."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
    except ValueError:
        raise UnsafeURLError("not a valid URL") from None
    if parsed.scheme not in ("http", "https"):
        raise UnsafeURLError("the URL must start with http:// or https://")
    if not host:
        raise UnsafeURLError("the URL has no host")
    return url


_NAT64 = ipaddress.ip_network("64:ff9b::/96")


def _is_public(address):
    """True only for an address on the public internet. An IPv6 address that wraps an IPv4 one is judged by the IPv4."""
    if address.version == 6:
        if address.ipv4_mapped is not None:
            return _is_public(address.ipv4_mapped)
        if address.sixtofour is not None:
            return _is_public(address.sixtofour)
        if address in _NAT64:
            return _is_public(ipaddress.ip_address(int(address) & 0xFFFFFFFF))
    return address.is_global and not address.is_multicast


async def resolve_public(host, port):
    """The address to connect to for `host`. Raises UnsafeURLError if the name gives any address that is not public.

    Every address is checked, not just the one used, so a name that mixes a public and a private answer is refused.
    """
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        raise UnsafeURLError(f"cannot look up {host}") from error
    addresses = []
    for info in infos:
        text = info[4][0].split("%", 1)[0]  # an IPv6 link-local address can carry a zone, "fe80::1%eth0"
        addresses.append(ipaddress.ip_address(text))
    if not addresses:
        raise UnsafeURLError(f"{host} has no address")
    if not config.ALLOW_PRIVATE_URLS:
        for address in addresses:
            if not _is_public(address):
                raise UnsafeURLError(f"{host} points to a private or internal address")
    return str(addresses[0])


class _GuardedBackend(httpcore.AnyIOBackend):
    """Connects to the address that passed the check. TLS still uses the host name from the URL, so the certificate is
    checked against the name the tenant gave."""

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        address = await resolve_public(host, port)
        return await super().connect_tcp(
            address, port, timeout=timeout, local_address=local_address, socket_options=socket_options
        )


def guarded_client(**kwargs):
    """An httpx client that refuses private addresses, does not follow redirects and ignores proxy settings in the
    environment. The pool's network backend is httpcore's own hook; httpx has no public way to set it, and the versions
    are pinned in requirements.txt and covered by tests/test_url_guard.py."""
    transport = httpx.AsyncHTTPTransport()
    transport._pool._network_backend = _GuardedBackend()
    return httpx.AsyncClient(transport=transport, trust_env=False, follow_redirects=False, **kwargs)
