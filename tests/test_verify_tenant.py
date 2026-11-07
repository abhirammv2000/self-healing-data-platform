"""The two checks every route sits behind: the admin secret, and the tenant API key.

verify_tenant is what keeps one tenant out of another tenant's pipelines, so each way it can say no is tested:
an unknown or revoked key (401), a valid key used on a different tenant (403), and a suspended tenant (403).
The database is faked. These tests check the function's own decisions.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from control_plane.app.dependencies import verify_admin, verify_tenant
from shared.config import ADMIN_SECRET_KEY


def session_returning(row):
    session = AsyncMock()
    session.execute = AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: row))
    return session


async def test_a_valid_key_for_its_own_active_tenant_passes():
    row = (SimpleNamespace(tenant_id=1), SimpleNamespace(is_active=True))
    assert await verify_tenant(tenant_id=1, x_api_key="good", session=session_returning(row)) is None


async def test_an_unknown_or_revoked_key_is_a_401():
    with pytest.raises(HTTPException) as caught:
        await verify_tenant(tenant_id=1, x_api_key="unknown", session=session_returning(None))
    assert caught.value.status_code == 401


async def test_a_valid_key_used_on_another_tenant_is_a_403():
    row = (SimpleNamespace(tenant_id=2), SimpleNamespace(is_active=True))
    with pytest.raises(HTTPException) as caught:
        await verify_tenant(tenant_id=1, x_api_key="other-tenants-key", session=session_returning(row))
    assert caught.value.status_code == 403


async def test_a_suspended_tenant_is_a_403_even_with_a_valid_key():
    row = (SimpleNamespace(tenant_id=1), SimpleNamespace(is_active=False))
    with pytest.raises(HTTPException) as caught:
        await verify_tenant(tenant_id=1, x_api_key="good", session=session_returning(row))
    assert caught.value.status_code == 403


async def test_the_key_is_looked_up_by_its_hash_and_never_by_the_raw_value():
    session = session_returning((SimpleNamespace(tenant_id=1), SimpleNamespace(is_active=True)))
    await verify_tenant(tenant_id=1, x_api_key="plain-text-key", session=session)
    query = str(session.execute.await_args.args[0].compile(compile_kwargs={"literal_binds": True}))
    assert "plain-text-key" not in query


async def test_the_admin_secret_must_match():
    assert await verify_admin(x_admin_secret=ADMIN_SECRET_KEY) is None
    with pytest.raises(HTTPException) as caught:
        await verify_admin(x_admin_secret="not-the-secret")
    assert caught.value.status_code == 401
