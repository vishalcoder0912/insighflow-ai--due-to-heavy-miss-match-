"""Advanced analytics engine tests."""

from __future__ import annotations

import io
from datetime import date, timedelta

import pytest

from tests.conftest import auth_headers, register_and_login


def _advanced_sales_csv(days: int = 90) -> io.BytesIO:
    rows = ["order_date,customer_id,region,revenue,orders,discount,channel"]
    start = date(2025, 1, 1)
    customers = [f"C{idx:03d}" for idx in range(1, 31)]
    regions = ["North", "South", "East", "West"]
    channels = ["web", "partner", "field"]

    for offset in range(days):
        current = start + timedelta(days=offset)
        customer = customers[offset % len(customers)]
        revenue = 1000 + (offset * 17) + ((offset % 7) * 45)
        orders = 1 + (offset % 5)
        discount = round((offset % 4) * 0.05, 2)
        region = regions[offset % len(regions)]
        channel = channels[offset % len(channels)]
        rows.append(f"{current.isoformat()},{customer},{region},{revenue},{orders},{discount},{channel}")
    return io.BytesIO("\n".join(rows).encode("utf-8"))


def _short_sales_csv(days: int = 10) -> io.BytesIO:
    return _advanced_sales_csv(days=days)


@pytest.mark.asyncio
async def test_advanced_analytics_run_returns_structured_results(client):
    auth_payload = await register_and_login(client, "advanced-owner@example.com")
    token = auth_payload["tokens"]["access_token"]

    upload_response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        files={"file": ("advanced_sales.csv", _advanced_sales_csv(), "text/csv")},
    )
    assert upload_response.status_code == 201, upload_response.text
    dataset_id = upload_response.json()["dataset_id"]

    response = await client.post(
        "/api/v1/analytics/advanced/run",
        headers=auth_headers(token),
        json={
            "dataset_id": dataset_id,
            "analyses": ["forecasting", "clustering", "regression", "cohort", "rfm"],
            "options": {"forecast_periods": 7, "max_clusters": 4},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] in {"SUCCESS", "PARTIAL_SUCCESS"}
    assert body["results"]["forecasting"]["results"]["forecast_points"]
    assert body["results"]["clustering"]["results"]["cluster_profiles"]
    assert body["results"]["regression"]["results"]["feature_importance"]
    assert body["results"]["cohort"]["results"]["retention_matrix"]
    assert body["results"]["rfm"]["results"]["segments"]
    assert body["ai_insights"]["key_findings"]


@pytest.mark.asyncio
async def test_advanced_analytics_handles_validation_failures_gracefully(client):
    auth_payload = await register_and_login(client, "advanced-short@example.com")
    token = auth_payload["tokens"]["access_token"]

    upload_response = await client.post(
        "/api/v1/datasets/upload",
        headers=auth_headers(token),
        files={"file": ("short_sales.csv", _short_sales_csv(), "text/csv")},
    )
    assert upload_response.status_code == 201, upload_response.text
    dataset_id = upload_response.json()["dataset_id"]

    response = await client.post(
        "/api/v1/analytics/advanced/run",
        headers=auth_headers(token),
        json={"dataset_id": dataset_id, "analyses": ["forecasting"]},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "FAILED"
    assert body["failures"][0]["analysis_type"] == "forecasting"
    assert body["failures"][0]["error_code"].startswith("VAL_")
