"""The worker must not be usable to call the cloud metadata service, the database or anything else inside its network.

Addresses and names are checked as data, and the real client is checked against a real local server: a private
address is refused before any connection, an allowed one is reached, and the connection goes to the address that was
checked.
"""
import asyncio
import http.server
import ipaddress
import socket
import threading
from types import SimpleNamespace

import httpx
import pytest

from shared import config, url_guard, webhook_dispatcher
from shared.url_guard import UnsafeURLError, check_url_syntax, guarded_client, resolve_public
from worker.app import step_handlers


# the URL as written

@pytest.mark.parametrize("url", ["http://example.com/a.csv", "https://example.com:8443/x?y=1", "http://203.0.113.5/"])
def test_http_and_https_urls_pass_the_syntax_check(url):
    assert check_url_syntax(url) == url


@pytest.mark.parametrize(
    "url", ["ftp://example.com/a", "file:///etc/passwd", "gopher://x/", "javascript:alert(1)", "example.com/a", "/hooks/run", "http://", "", "http:///path"]
)
def test_other_schemes_and_hostless_urls_are_refused(url):
    with pytest.raises(UnsafeURLError):
        check_url_syntax(url)


# the address a name leads to

PUBLIC = ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111", "::ffff:8.8.8.8"]
NOT_PUBLIC = [
    "127.0.0.1", "127.1.2.3", "10.0.0.5", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254", "169.254.0.1",
    "100.64.0.1", "0.0.0.0", "255.255.255.255", "224.0.0.1", "192.0.2.1", "198.18.0.1",
    "::1", "::", "fe80::1", "fc00::1", "fd12:3456::1", "ff02::1",
    "::ffff:127.0.0.1", "::ffff:169.254.169.254", "::ffff:10.0.0.1",
    "2002:a9fe:a9fe::1",  # 6to4 wrapping 169.254.169.254
    "64:ff9b::a9fe:a9fe",  # NAT64 wrapping 169.254.169.254
    "64:ff9b::7f00:1",  # NAT64 wrapping 127.0.0.1
]


@pytest.mark.parametrize("text", PUBLIC)
def test_public_addresses_are_public(text):
    assert url_guard._is_public(ipaddress.ip_address(text))


@pytest.mark.parametrize("text", NOT_PUBLIC)
def test_private_internal_and_wrapped_addresses_are_not_public(text):
    assert not url_guard._is_public(ipaddress.ip_address(text))


def fake_lookup(monkeypatch, *addresses, error=None):
    """Make every name resolve to `addresses`, and count how many lookups were made."""
    calls = []

    async def getaddrinfo(host, port, **kwargs):
        calls.append(host)
        if error:
            raise error
        return [(socket.AF_INET6 if ":" in a else socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, port)) for a in addresses]

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", getaddrinfo)
    return calls


async def test_a_name_that_leads_to_a_public_address_is_resolved(monkeypatch):
    fake_lookup(monkeypatch, "93.184.216.34")
    assert await resolve_public("example.com", 80) == "93.184.216.34"


@pytest.mark.parametrize("address", ["127.0.0.1", "10.1.2.3", "169.254.169.254", "::1", "fd00::1"])
async def test_a_name_that_leads_to_a_private_address_is_refused(monkeypatch, address):
    fake_lookup(monkeypatch, address)
    with pytest.raises(UnsafeURLError, match="private or internal"):
        await resolve_public("metadata.internal.example", 80)


async def test_one_private_answer_among_public_ones_refuses_the_name(monkeypatch):
    fake_lookup(monkeypatch, "93.184.216.34", "10.0.0.9")
    with pytest.raises(UnsafeURLError):
        await resolve_public("mixed.example", 80)


async def test_a_name_that_cannot_be_looked_up_is_refused(monkeypatch):
    fake_lookup(monkeypatch, error=socket.gaierror("no such host"))
    with pytest.raises(UnsafeURLError, match="cannot look up"):
        await resolve_public("nope.example", 80)


async def test_a_zone_on_an_ipv6_address_does_not_hide_a_private_address(monkeypatch):
    fake_lookup(monkeypatch, "fe80::1%eth0")
    with pytest.raises(UnsafeURLError):
        await resolve_public("linklocal.example", 80)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "2130706433", "0x7f000001", "017700000001", "127.1", "[::1]"])
async def test_every_way_of_writing_loopback_is_refused_by_the_real_resolver(host):
    # no fake: this is what the operating system's resolver makes of each spelling
    try:
        await resolve_public(host.strip("[]"), 80)
    except UnsafeURLError:
        return
    pytest.fail(f"{host} was accepted")


async def test_private_addresses_are_allowed_when_the_switch_is_on(monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    fake_lookup(monkeypatch, "10.1.2.3")
    assert await resolve_public("intranet.example", 80) == "10.1.2.3"


def test_private_addresses_are_refused_by_default():
    assert config.ALLOW_PRIVATE_URLS is False


# the real client, against a real server

class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()
            return
        if self.path == "/big":
            size = 3 * 1024 * 1024
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.end_headers()
            try:
                self.wfile.write(b"x" * size)
            except OSError:
                pass
            return
        if self.path == "/endless":
            self.send_response(200)  # no Content-Length: the size is not known up front
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b"x" * 65536)
            except OSError:
                pass
            return
        body = b"id,value\n1,10\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


async def test_the_client_refuses_a_private_address_before_connecting(server):
    async with guarded_client() as client:
        with pytest.raises(UnsafeURLError):
            await client.get(server + "/ok.csv")


async def test_the_client_reaches_a_private_address_when_the_switch_is_on(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    async with guarded_client() as client:
        assert (await client.get(server + "/ok.csv")).status_code == 200


async def test_the_connection_goes_to_the_address_that_was_looked_up(server, monkeypatch):
    # "pinned.invalid" cannot be resolved by anyone: it only works if the connection uses the answer of the lookup
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    calls = fake_lookup(monkeypatch, "127.0.0.1")
    port = server.rsplit(":", 1)[1]
    async with guarded_client() as client:
        response = await client.get(f"http://pinned.invalid:{port}/ok.csv")
    assert response.status_code == 200
    assert calls == ["pinned.invalid"]  # looked up once, so the name cannot lead somewhere else afterwards


async def test_a_name_that_changes_its_answer_cannot_reach_a_private_address(server, monkeypatch):
    # DNS rebinding: public the first time it is asked, private after. It is asked once per connection, and the
    # answer that is checked is the one that is used
    answers = iter([["93.184.216.34"], ["127.0.0.1"]])

    async def getaddrinfo(host, port, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, port)) for a in next(answers)]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)
    port = server.rsplit(":", 1)[1]
    seen = []
    real_connect = url_guard.httpcore.AnyIOBackend.connect_tcp

    async def spy(self, host, port, **kwargs):
        seen.append(host)
        raise url_guard.httpcore.ConnectError("not really connecting")  # do not connect to the public address

    monkeypatch.setattr(url_guard.httpcore.AnyIOBackend, "connect_tcp", spy)
    async with guarded_client() as client:
        with pytest.raises(httpx.ConnectError):
            await client.get(f"http://rebind.invalid:{port}/ok.csv")
    monkeypatch.setattr(url_guard.httpcore.AnyIOBackend, "connect_tcp", real_connect)
    assert seen == ["93.184.216.34"]  # the checked address was the one connected to


async def test_environment_proxies_are_ignored(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://10.0.0.1:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://10.0.0.1:3128")
    async with guarded_client() as client:
        assert isinstance(client._transport, httpx.AsyncHTTPTransport)
        assert client._mounts == {}


# fetch_data

async def test_fetch_data_returns_the_file(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    assert await step_handlers.fetch_data(server + "/ok.csv") == b"id,value\n1,10\n"


async def test_fetch_data_refuses_a_private_address_by_default(server):
    with pytest.raises(UnsafeURLError):
        await step_handlers.fetch_data(server + "/ok.csv")


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/a", "169.254.169.254/latest"])
async def test_fetch_data_refuses_other_schemes(url):
    with pytest.raises(UnsafeURLError):
        await step_handlers.fetch_data(url)


async def test_fetch_data_does_not_follow_a_redirect_to_the_metadata_service(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    with pytest.raises(httpx.HTTPStatusError):
        await step_handlers.fetch_data(server + "/redirect")


async def test_fetch_data_refuses_a_file_larger_than_the_limit_by_its_declared_size(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    monkeypatch.setattr(config, "MAX_DOWNLOAD_MB", 1)
    with pytest.raises(ValueError, match="larger than 1 MB"):
        await step_handlers.fetch_data(server + "/big")


async def test_fetch_data_stops_reading_an_answer_of_unknown_size_at_the_limit(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    monkeypatch.setattr(config, "MAX_DOWNLOAD_MB", 1)
    with pytest.raises(ValueError, match="larger than 1 MB"):
        await step_handlers.fetch_data(server + "/endless")


async def test_fetch_data_accepts_a_file_under_the_limit(server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    monkeypatch.setattr(config, "MAX_DOWNLOAD_MB", 4)
    assert len(await step_handlers.fetch_data(server + "/big")) == 3 * 1024 * 1024


async def test_a_blocked_source_fails_the_ingestion_step_with_the_reason(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(step_handlers.IngestionStepError, match="private or internal"):
        await step_handlers.run_ingestion({"source_url": "http://169.254.169.254/latest/meta-data/"}, {"run_id": 1})


# the webhook

class _Session:
    def __init__(self):
        self.added = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def add(self, row):
        self.added.append(row)

    async def commit(self):
        pass

    async def refresh(self, row):
        pass

    async def rollback(self):
        pass


@pytest.fixture
def audit(monkeypatch):
    session = _Session()
    monkeypatch.setattr(webhook_dispatcher, "async_session", lambda: session)
    monkeypatch.setattr(webhook_dispatcher, "WebhookCallback", lambda **fields: SimpleNamespace(**fields))
    return session


async def test_a_webhook_to_a_private_address_is_not_sent_and_is_recorded_as_failed(audit, server):
    result = await webhook_dispatcher.dispatch_webhook_callback(server + "/hook", 1, 2, 3, "failed")
    assert result.status == "failed"
    assert "private or internal" in result.error_message
    assert getattr(result, "http_status_code", None) is None


async def test_a_webhook_to_a_non_http_url_is_recorded_as_failed(audit):
    result = await webhook_dispatcher.dispatch_webhook_callback("file:///etc/passwd", 1, 2, 3, "failed")
    assert result.status == "failed" and "http" in result.error_message


async def test_a_webhook_is_delivered_when_private_addresses_are_allowed(audit, server, monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_URLS", True)
    result = await webhook_dispatcher.dispatch_webhook_callback(server + "/hook", 1, 2, 3, "success")
    assert result.status == "success" and result.http_status_code == 204


# the API refuses a webhook that cannot work

from pydantic import ValidationError

from control_plane.app.schemas.pipelines import PipelineCreate, PipelineUpdate


def test_a_pipeline_accepts_an_http_webhook_or_none():
    assert PipelineCreate(name="p", callback_url="https://hooks.example.com/run").callback_url == "https://hooks.example.com/run"
    assert PipelineCreate(name="p").callback_url is None
    assert PipelineUpdate(callback_url=None).callback_url is None


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com", "/hooks/run", "example.com/hook", "http://"])
def test_a_pipeline_refuses_a_webhook_that_is_not_http(url):
    with pytest.raises(ValidationError):
        PipelineCreate(name="p", callback_url=url)
    with pytest.raises(ValidationError):
        PipelineUpdate(callback_url=url)
