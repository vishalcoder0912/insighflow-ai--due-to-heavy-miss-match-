"""Project, insight, and analytics tests."""

from __future__ import annotations

import pytest

from tests.conftest import auth_headers, register_and_login


@pytest.mark.asyncio
async def test_project_insight_and_analytics_flow(client):
    owner_auth = await register_and_login(client, "owner@example.com")
    viewer_auth = await register_and_login(client, "viewer@example.com")
    owner_token = owner_auth["tokens"]["access_token"]
    viewer_user_id = viewer_auth["user"]["id"]

    project_response = await client.post(
        "/api/v1/projects",
        json={"name": "Revenue Ops", "description": "Q1 metrics"},
        headers=auth_headers(owner_token),
    )
    assert project_response.status_code == 201, project_response.text
    project_id = project_response.json()["id"]

    share_response = await client.post(
        f"/api/v1/projects/{project_id}/share",
        json={"user_id": viewer_user_id, "permission": "viewer"},
        headers=auth_headers(owner_token),
    )
    assert share_response.status_code == 200, share_response.text

    insight_response = await client.post(
        "/api/v1/insights",
        json={
            "project_id": project_id,
            "title": "Revenue Growth",
            "summary": "Monthly increase",
            "content": "Revenue grew strongly in March due to expanded enterprise deals.",
            "status": "published",
            "severity": "high",
            "tags": ["sales", "growth"],
        },
        headers=auth_headers(owner_token),
    )
    assert insight_response.status_code == 201, insight_response.text

    viewer_projects = await client.get("/api/v1/projects", headers=auth_headers(viewer_auth["tokens"]["access_token"]))
    assert viewer_projects.status_code == 200
    assert viewer_projects.json()["pagination"]["total"] == 1

    analytics_response = await client.get(f"/api/v1/projects/{project_id}/analytics", headers=auth_headers(owner_token))
    assert analytics_response.status_code == 200
    body = analytics_response.json()
    assert body["total_insights"] == 1
    assert body["status_breakdown"]["published"] == 1

    dashboard_response = await client.get("/api/v1/analytics/dashboard", headers=auth_headers(owner_token))
    assert dashboard_response.status_code == 200
    assert dashboard_response.json()["total_projects"] == 1

    filtered_response = await client.get(
        "/api/v1/insights",
        params={"status_filter": "published"},
        headers=auth_headers(owner_token),
    )
    assert filtered_response.status_code == 200
    assert filtered_response.json()["pagination"]["total"] == 1
