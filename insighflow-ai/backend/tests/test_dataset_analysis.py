"""Dataset analysis tests."""

from __future__ import annotations

import io

import pytest

from tests.conftest import auth_headers, register_and_login


def _sales_csv() -> io.BytesIO:
    return io.BytesIO(
        b"order_date,customer_id,region,revenue,orders\n"
        b"2025-01-01,C001,North,1200,3\n"
        b"2025-02-01,C002,South,1800,4\n"
        b"2025-03-01,C001,North,2200,5\n"
    )


@pytest.mark.asyncio
async def test_dataset_analysis_upload_returns_blueprint(client):
    auth_payload = await register_and_login(client)
    token = auth_payload["tokens"]["access_token"]

    project_response = await client.post(
        "/api/v1/projects",
        json={"name": "Sales Dataset Project", "description": "Analyze uploaded sales data"},
        headers=auth_headers(token),
    )
    project_id = project_response.json()["id"]

    response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        data={"project_id": str(project_id)},
        files={"file": ("sales.csv", _sales_csv(), "text/csv")},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["detected_domain"] == "sales"
    assert body["row_count"] == 3
    assert body["schema"]
    assert body["data_preview"]["sample_rows"]
    assert body["data_preview"]["column_headers"]


@pytest.mark.asyncio
async def test_dataset_reanalysis_and_derived_endpoints(client):
    auth_payload = await register_and_login(client, "analysis-owner@example.com")
    token = auth_payload["tokens"]["access_token"]

    upload_response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        files={"file": ("sales.csv", _sales_csv(), "text/csv")},
    )
    assert upload_response.status_code == 201, upload_response.text
    dataset_id = upload_response.json()["dataset_id"]

    analyze_response = await client.post(
        f"/api/v1/datasets/{dataset_id}/analyze",
        headers=auth_headers(token),
    )
    assert analyze_response.status_code == 200, analyze_response.text
    analysis_body = analyze_response.json()
    assert analysis_body["detected_domain"] == "sales"
    assert analysis_body["recommended_kpis"]
    assert analysis_body["chart_recommendations"]
    assert analysis_body["dashboard_layout"]["components"]
    assert analysis_body["ai_insights"]["key_findings"]

    preview_response = await client.get(f"/api/v1/datasets/{dataset_id}/preview", headers=auth_headers(token))
    assert preview_response.status_code == 200, preview_response.text
    preview_body = preview_response.json()
    assert preview_body["sample_rows"]
    assert preview_body["column_headers"]
    assert "revenue" in preview_body["mini_histograms"]
    assert preview_body["correlation_matrix"]

    statistics_response = await client.get(f"/api/v1/datasets/{dataset_id}/statistics", headers=auth_headers(token))
    assert statistics_response.status_code == 200, statistics_response.text
    statistics_body = statistics_response.json()
    assert statistics_body["statistical_summary"]["revenue"]["mean"] == 1733.3333
    assert statistics_body["statistical_summary"]["revenue"]["mode"] == 1200.0
    assert statistics_body["statistical_analysis"]["correlations"]
    assert statistics_body["quality_report"]["completeness_score"] == 100.0

    insights_response = await client.get(f"/api/v1/datasets/{dataset_id}/insights", headers=auth_headers(token))
    assert insights_response.status_code == 200, insights_response.text
    insights_body = insights_response.json()
    assert insights_body["ai_insights"]["key_findings"]
    assert "risks" in insights_body["ai_insights"]
    assert insights_body["ai_insights"]["recommendations"]


@pytest.mark.asyncio
async def test_dashboard_generation_persists_blueprint(client):
    auth_payload = await register_and_login(client, "dashboard-owner@example.com")
    token = auth_payload["tokens"]["access_token"]

    upload_response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        files={"file": ("sales.csv", _sales_csv(), "text/csv")},
    )
    dataset_id = upload_response.json()["dataset_id"]

    analysis_response = await client.post(
        f"/api/v1/datasets/{dataset_id}/analyze",
        headers=auth_headers(token),
    )
    analysis_body = analysis_response.json()
    selected_kpi = analysis_body["recommended_kpis"][0]["id"]

    generate_response = await client.post(
        "/api/v1/dashboards/generate",
        headers=auth_headers(token),
        json={"dataset_id": dataset_id, "custom_kpi_selections": [selected_kpi], "name": "Exec Dashboard"},
    )
    assert generate_response.status_code == 201, generate_response.text
    generate_body = generate_response.json()
    assert generate_body["blueprint"]["recommended_kpis"]
    assert len(generate_body["blueprint"]["recommended_kpis"]) == 1
    assert generate_body["blueprint"]["dashboard_layout"]["components"]

    dashboard_id = generate_body["dashboard_id"]
    blueprint_response = await client.get(
        f"/api/v1/dashboards/{dashboard_id}/blueprint",
        headers=auth_headers(token),
    )
    assert blueprint_response.status_code == 200, blueprint_response.text
    blueprint_body = blueprint_response.json()
    assert blueprint_body["blueprint"]["dataset_id"] == dataset_id
    assert blueprint_body["blueprint"]["ai_insights"]["key_findings"]


@pytest.mark.asyncio
async def test_json_dataset_with_nested_values_is_handled(client):
    auth_payload = await register_and_login(client, "json-owner@example.com")
    token = auth_payload["tokens"]["access_token"]

    response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        files={
            "file": (
                "nested.json",
                io.BytesIO(
                    b'[{"customer":"A1","revenue":100,"tags":["vip","north"],"meta":{"channel":"web"}},'
                    b'{"customer":"B2","revenue":150,"tags":["south"],"meta":{"channel":"partner"}}]'
                ),
                "application/json",
            )
        },
    )
    assert response.status_code == 201, response.text
    preview_rows = response.json()["data_preview"]["sample_rows"]
    assert isinstance(preview_rows[0]["tags"], list)
    assert isinstance(preview_rows[0]["meta"], dict)
