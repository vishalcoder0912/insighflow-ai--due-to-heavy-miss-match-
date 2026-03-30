"""Authentication tests."""

from __future__ import annotations

import pytest

from tests.conftest import auth_headers, register_and_login


@pytest.mark.asyncio
async def test_register_login_refresh_logout_flow(client):
    auth_payload = await register_and_login(client)
    refresh_token = auth_payload["tokens"]["refresh_token"]

    login_response = await client.post(
        "/api/v1/auth/login",
        json={"email": "owner@example.com", "password": "StrongPass123"},
    )
    assert login_response.status_code == 200

    refresh_response = await client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_token})
    assert refresh_response.status_code == 200
    new_access_token = refresh_response.json()["tokens"]["access_token"]

    me_response = await client.get("/api/v1/users/me", headers=auth_headers(new_access_token))
    assert me_response.status_code == 200
    assert me_response.json()["email"] == "owner@example.com"

    logout_response = await client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": refresh_response.json()["tokens"]["refresh_token"]},
        headers=auth_headers(new_access_token),
    )
    assert logout_response.status_code == 204


@pytest.mark.asyncio
async def test_logout_cannot_revoke_another_users_refresh_token(client):
    owner_auth = await register_and_login(client, "owner@example.com")
    viewer_auth = await register_and_login(client, "viewer@example.com")

    response = await client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": viewer_auth["tokens"]["refresh_token"]},
        headers=auth_headers(owner_auth["tokens"]["access_token"]),
    )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "refresh_token_forbidden"
