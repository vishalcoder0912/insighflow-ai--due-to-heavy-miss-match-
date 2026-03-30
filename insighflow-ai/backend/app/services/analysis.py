"""Dataset ingestion, profiling, and dashboard recommendation engine."""

from __future__ import annotations

import asyncio
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import chardet
import numpy as np
import pandas as pd
from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

try:
    import spacy
except Exception:  # pragma: no cover - optional local NLP dependency
    spacy = None

try:
    from scipy.stats import kurtosis as scipy_kurtosis
    from scipy.stats import mode as scipy_mode
    from scipy.stats import normaltest, skew as scipy_skew
except ImportError:  # pragma: no cover - optional local stats dependency
    scipy_kurtosis = None
    scipy_mode = None
    normaltest = None
    scipy_skew = None

try:
    from sklearn.ensemble import IsolationForest
except ImportError:  # pragma: no cover - optional local ML dependency
    IsolationForest = None

try:
    from pyod.models.iforest import IForest
except ImportError:  # pragma: no cover - optional local anomaly dependency
    IForest = None

try:
    from statsmodels.tsa.stattools import acf
except ImportError:  # pragma: no cover - optional local time-series dependency
    acf = None

from app.core.exceptions import ApiException
from app.models.dataset import DatasetAsset
from app.models.user import User
from app.services.audit import log_audit_event
from app.services.projects import get_project_or_404
from app.utils.files import save_upload

DOMAIN_THEMES: dict[str, dict[str, Any]] = {
    "sales": {"primary": "#0f766e", "accent": "#f59e0b", "surface": "#f0fdfa", "positive": "#16a34a", "negative": "#dc2626"},
    "hr": {"primary": "#1d4ed8", "accent": "#9333ea", "surface": "#eff6ff", "positive": "#16a34a", "negative": "#dc2626"},
    "financial": {"primary": "#166534", "accent": "#dc2626", "surface": "#f0fdf4", "positive": "#16a34a", "negative": "#dc2626"},
    "operational": {"primary": "#334155", "accent": "#0ea5e9", "surface": "#f8fafc", "positive": "#16a34a", "negative": "#dc2626"},
    "customer": {"primary": "#7c2d12", "accent": "#0ea5e9", "surface": "#fff7ed", "positive": "#16a34a", "negative": "#dc2626"},
    "inventory": {"primary": "#4338ca", "accent": "#ea580c", "surface": "#eef2ff", "positive": "#16a34a", "negative": "#dc2626"},
    "time_series": {"primary": "#1f2937", "accent": "#10b981", "surface": "#f9fafb", "positive": "#16a34a", "negative": "#dc2626"},
}

DOMAIN_KEYWORDS: dict[str, set[str]] = {
    "sales": {"sales", "revenue", "order", "orders", "customer", "product", "profit", "region", "transaction", "amount"},
    "hr": {"employee", "salary", "department", "position", "headcount", "payroll", "tenure", "attrition", "role", "compensation"},
    "financial": {"income", "expense", "budget", "cash", "asset", "liability", "profit", "margin", "roi", "forecast"},
    "operational": {"efficiency", "productivity", "throughput", "response", "cycle", "error", "quality", "compliance", "utilization"},
    "customer": {"customer", "segment", "churn", "retention", "nps", "csat", "lifetime", "repeat", "demographic", "tenure"},
    "inventory": {"inventory", "stock", "warehouse", "sku", "supply", "demand", "reorder", "supplier"},
    "time_series": {"date", "day", "week", "month", "quarter", "year", "timestamp", "period"},
}

VALUE_COLUMN_HINTS = ["revenue", "sales", "amount", "income", "expense", "cost", "salary", "payroll", "price", "profit"]
DATE_COLUMN_HINTS = ["date", "time", "timestamp", "month", "year", "quarter", "week", "day"]

ROLE_HINTS: dict[str, dict[str, Any]] = {
    "date": {"keywords": ["date", "time", "timestamp", "month", "year", "quarter", "week", "day"], "type": "temporal"},
    "revenue": {"keywords": ["revenue", "sales", "income", "gmv", "arr"], "type": "numerical"},
    "orders": {"keywords": ["order", "orders", "transactions", "transaction", "bookings"], "type": "numerical"},
    "customer": {"keywords": ["customer", "client", "account", "buyer", "user"], "type": None},
    "product": {"keywords": ["product", "item", "sku", "service", "category"], "type": None},
    "region": {"keywords": ["region", "country", "state", "city", "territory", "market"], "type": None},
    "salary": {"keywords": ["salary", "compensation", "payroll", "wage", "pay"], "type": "numerical"},
    "employee": {"keywords": ["employee", "staff", "worker", "associate", "personnel"], "type": None},
    "department": {"keywords": ["department", "team", "business_unit", "function"], "type": None},
    "tenure": {"keywords": ["tenure", "years_of_service", "service_years", "experience"], "type": "numerical"},
    "attrition": {"keywords": ["attrition", "turnover", "terminated", "left_company"], "type": None},
    "expense": {"keywords": ["expense", "cost", "spend", "opex"], "type": "numerical"},
    "budget": {"keywords": ["budget", "plan", "target"], "type": "numerical"},
    "profit": {"keywords": ["profit", "ebitda", "net_income"], "type": "numerical"},
    "cash": {"keywords": ["cash", "cashflow", "cash_flow"], "type": "numerical"},
    "efficiency": {"keywords": ["efficiency", "productivity", "utilization", "uptime"], "type": "numerical"},
    "error_rate": {"keywords": ["error", "defect", "failure", "incident", "bug"], "type": "numerical"},
    "response_time": {"keywords": ["response", "latency", "cycle", "duration", "sla"], "type": "numerical"},
    "throughput": {"keywords": ["throughput", "volume", "output", "processed"], "type": "numerical"},
    "compliance": {"keywords": ["compliance", "quality", "pass_rate"], "type": "numerical"},
    "churn": {"keywords": ["churn", "cancel", "attrition_rate"], "type": "numerical"},
    "nps": {"keywords": ["nps", "csat", "satisfaction", "score"], "type": "numerical"},
    "lifetime_value": {"keywords": ["lifetime_value", "ltv", "clv"], "type": "numerical"},
    "age": {"keywords": ["age"], "type": "numerical"},
    "inventory": {"keywords": ["inventory", "stock", "on_hand", "quantity", "qty"], "type": "numerical"},
    "supplier": {"keywords": ["supplier", "vendor"], "type": None},
}


@lru_cache
def _get_nlp():
    if spacy is None:
        return None
    for model_name in ("en_core_web_sm",):
        try:
            return spacy.load(model_name, disable=["parser", "ner"])
        except OSError:
            continue
    return spacy.blank("en")


def _tokenize_text(text: str) -> list[str]:
    normalized = text.strip().lower()
    if not normalized:
        return []
    nlp = _get_nlp()
    if nlp is None:
        return [token for token in normalized.replace("_", " ").split() if token]
    doc = nlp(normalized.replace("_", " "))
    tokens = []
    for token in doc:
        if token.is_stop or token.is_punct or token.is_space:
            continue
        lemma = token.lemma_.strip().lower() if token.lemma_ else token.text.strip().lower()
        if lemma:
            tokens.append(lemma)
    return tokens


def _normalize_column_name(name: str) -> str:
    return str(name).strip()


def _is_missing_value(value: Any) -> bool:
    if isinstance(value, (dict, list, tuple, set)):
        return False
    result = pd.isna(value)
    return bool(result) if isinstance(result, (bool, np.bool_)) else False


def _safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item) for item in value]
    if isinstance(value, (np.generic,)):
        return value.item()
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if _is_missing_value(value):
        return None
    return value


def _detect_encoding(raw_bytes: bytes) -> str:
    detected = chardet.detect(raw_bytes[:50000]) or {}
    return detected.get("encoding") or "utf-8"


def _load_dataframe(file_path: Path, sniff_bytes: bytes) -> tuple[pd.DataFrame, str | None]:
    suffix = file_path.suffix.lower()
    encoding: str | None = None
    if suffix == ".csv":
        encoding = _detect_encoding(sniff_bytes)
        dataframe = pd.read_csv(file_path, encoding=encoding, low_memory=False)
    elif suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(file_path)
    elif suffix == ".json":
        encoding = _detect_encoding(sniff_bytes)
        try:
            dataframe = pd.read_json(file_path)
        except ValueError:
            with file_path.open("r", encoding=encoding) as handle:
                dataframe = pd.DataFrame(json.load(handle))
    elif suffix == ".parquet":
        dataframe = pd.read_parquet(file_path)
    else:
        raise ApiException(status_code=400, code="unsupported_file_type", message="Unsupported dataset format.")

    dataframe.columns = [_normalize_column_name(column) for column in dataframe.columns]
    if dataframe.empty:
        raise ApiException(status_code=400, code="empty_dataset", message="Uploaded dataset is empty.")
    return dataframe, encoding


def _cardinality_label(unique_count: int, row_count: int) -> str:
    ratio = unique_count / row_count if row_count else 0
    if unique_count <= 10 or ratio <= 0.1:
        return "low"
    if unique_count <= 50 or ratio <= 0.5:
        return "medium"
    return "high"


def _detect_frequency(values: pd.Series) -> str | None:
    timestamps = pd.to_datetime(values, errors="coerce").dropna().sort_values().drop_duplicates()
    if len(timestamps) < 3:
        return None
    diffs = timestamps.diff().dropna().dt.total_seconds() / 86400
    median_days = float(diffs.median()) if not diffs.empty else 0.0
    if median_days <= 1.5:
        return "daily"
    if median_days <= 8:
        return "weekly"
    if median_days <= 32:
        return "monthly"
    if median_days <= 100:
        return "quarterly"
    return "yearly"


def _numeric_stats(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {}
    skewness = float(scipy_skew(numeric, bias=False)) if scipy_skew is not None and len(numeric) > 2 else (float(numeric.skew()) if len(numeric) > 2 else 0.0)
    kurtosis_value = float(scipy_kurtosis(numeric, fisher=True, bias=False)) if scipy_kurtosis is not None and len(numeric) > 3 else 0.0
    normality_p_value = float(normaltest(numeric).pvalue) if normaltest is not None and len(numeric) >= 8 else None
    q1 = float(numeric.quantile(0.25))
    q3 = float(numeric.quantile(0.75))
    iqr = q3 - q1
    outlier_count = int(((numeric < q1 - 1.5 * iqr) | (numeric > q3 + 1.5 * iqr)).sum()) if iqr else 0
    coefficient_variation = float(numeric.std(ddof=0) / numeric.mean()) if len(numeric) > 1 and numeric.mean() != 0 else 0.0
    distribution = "normal" if abs(skewness) <= 0.5 else ("right_skewed" if skewness > 0 else "left_skewed")
    mode_value: Any
    if scipy_mode is not None:
        mode_result = scipy_mode(numeric, keepdims=False)
        mode_value = _safe_value(mode_result.mode) if hasattr(mode_result, "mode") else None
    else:
        mode_series = numeric.mode()
        mode_value = _safe_value(mode_series.iloc[0]) if not mode_series.empty else None
    return {
        "min": _safe_value(numeric.min()),
        "max": _safe_value(numeric.max()),
        "mean": round(float(numeric.mean()), 4),
        "median": round(float(numeric.median()), 4),
        "mode": mode_value,
        "std_dev": round(float(numeric.std(ddof=0)), 4) if len(numeric) > 1 else 0.0,
        "variance": round(float(numeric.var(ddof=0)), 4) if len(numeric) > 1 else 0.0,
        "outlier_count": outlier_count,
        "distribution": distribution,
        "skewness": round(skewness, 4),
        "kurtosis": round(kurtosis_value, 4),
        "normality_p_value": round(normality_p_value, 4) if normality_p_value is not None else None,
        "coefficient_of_variation": round(coefficient_variation, 4),
    }


def _temporal_stats(values: pd.Series) -> dict[str, Any]:
    timestamps = pd.to_datetime(values, errors="coerce").dropna()
    if timestamps.empty:
        return {}
    return {
        "min": _safe_value(timestamps.min()),
        "max": _safe_value(timestamps.max()),
        "frequency": _detect_frequency(timestamps),
        "timespan_days": int((timestamps.max() - timestamps.min()).days) if len(timestamps) > 1 else 0,
    }


def _categorical_stats(values: pd.Series) -> dict[str, Any]:
    text_values = values.dropna().astype(str)
    if text_values.empty:
        return {}
    top_values = text_values.value_counts().head(5)
    return {
        "top_values": [{"label": label, "count": int(count)} for label, count in top_values.items()],
        "average_length": round(float(text_values.str.len().mean()), 2),
    }


def _infer_column_type(series: pd.Series, column_name: str) -> tuple[str, str | None, dict[str, Any]]:
    non_null = series.dropna()
    if non_null.empty:
        return "categorical", "unknown", {}

    lower_name = column_name.lower()
    string_values = non_null.astype(str).str.strip()
    lower_values = set(string_values.str.lower().head(100).tolist())
    boolean_literals = {"true", "false", "yes", "no", "0", "1", "y", "n"}
    if lower_values and lower_values.issubset(boolean_literals):
        return "boolean", "binary", {"true_like_values": sorted(lower_values)}

    numeric_values = pd.to_numeric(non_null, errors="coerce")
    numeric_ratio = float(numeric_values.notna().mean()) if len(non_null) else 0.0
    if numeric_ratio >= 0.9:
        numeric_values = numeric_values.dropna()
        subtype = "integer" if np.allclose(numeric_values % 1, 0) else "float"
        if any(token in lower_name for token in {"percent", "percentage", "rate", "ratio", "margin"}):
            subtype = "percentage"
        elif any(token in lower_name for token in {"revenue", "sales", "amount", "price", "cost", "salary", "income", "expense"}):
            subtype = "currency"
        return "numerical", subtype, _numeric_stats(non_null)

    datetime_values = pd.to_datetime(non_null, errors="coerce")
    datetime_ratio = float(datetime_values.notna().mean()) if len(non_null) else 0.0
    if datetime_ratio >= 0.8 or any(token in lower_name for token in DATE_COLUMN_HINTS):
        subtype = "datetime" if any(token in lower_name for token in {"time", "timestamp"}) else "date"
        return "temporal", subtype, _temporal_stats(non_null)

    if any(token in lower_name for token in {"country", "state", "city", "region", "lat", "lon", "latitude", "longitude"}):
        return "geolocation", "region", _categorical_stats(non_null)

    average_length = float(string_values.str.len().mean())
    subtype = "category" if average_length < 40 else "text"
    if any(token in lower_name for token in {"id", "code", "sku"}):
        subtype = "code"
    return "categorical", subtype, _categorical_stats(non_null)


def _profile_columns(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    row_count = len(dataframe)
    for column in dataframe.columns:
        series = dataframe[column]
        inferred_type, subtype, stats = _infer_column_type(series, column)
        unique_count = int(series.nunique(dropna=True))
        missing_percentage = round(float(series.isna().mean() * 100), 2)
        profiles.append(
            {
                "name": column,
                "inferred_type": inferred_type,
                "subtype": subtype,
                "unique_count": unique_count,
                "missing_percentage": missing_percentage,
                "cardinality": _cardinality_label(unique_count, row_count),
                "sample_values": [_safe_value(value) for value in series.dropna().astype(str).head(5).tolist()],
                "stats": stats,
            }
        )
    return profiles


def _infer_relationships(dataframe: pd.DataFrame, profiles: list[dict[str, Any]], row_count: int) -> list[dict[str, Any]]:
    relationships: list[dict[str, Any]] = []
    for profile in profiles:
        name = profile["name"].lower()
        unique_count = profile["unique_count"]
        if name == "id" or name.endswith("_id"):
            entity = name.removesuffix("_id") if name.endswith("_id") else "record"
            relationship_type = "primary_key_candidate" if unique_count >= max(1, row_count - 1) else "foreign_key_candidate"
            relationships.append(
                {
                    "column": profile["name"],
                    "entity": entity,
                    "relationship_type": relationship_type,
                    "confidence": 0.9 if relationship_type == "primary_key_candidate" else 0.75,
                }
            )

    numeric_columns = [profile["name"] for profile in profiles if profile["inferred_type"] == "numerical"]
    if len(numeric_columns) >= 2:
        correlation_frame = dataframe[numeric_columns].apply(pd.to_numeric, errors="coerce")
        matrix = correlation_frame.corr(numeric_only=True)
        seen_pairs: set[tuple[str, str]] = set()
        for left in matrix.columns:
            for right in matrix.columns:
                if left == right or (right, left) in seen_pairs:
                    continue
                seen_pairs.add((left, right))
                corr = matrix.loc[left, right]
                if pd.notna(corr) and abs(float(corr)) >= 0.65:
                    relationships.append(
                        {
                            "left_column": left,
                            "right_column": right,
                            "relationship_type": "statistical_correlation",
                            "confidence": round(abs(float(corr)), 2),
                            "direction": "positive" if corr > 0 else "negative",
                        }
                    )
    return relationships[:12]


def _detect_domain(dataframe: pd.DataFrame, profiles: list[dict[str, Any]], role_map: dict[str, str]) -> dict[str, Any]:
    names = " ".join(column.lower() for column in dataframe.columns)
    sample_blob = " ".join(
        " ".join(profile["sample_values"][:3]).lower()
        for profile in profiles
        if profile["inferred_type"] in {"categorical", "geolocation"}
    )
    lexical_tokens = set(_tokenize_text(names))
    sample_tokens = set(_tokenize_text(sample_blob))
    scores: dict[str, float] = {}
    signals: dict[str, list[str]] = {}
    temporal_columns = [p for p in profiles if p["inferred_type"] == "temporal"]
    numerical_columns = [p for p in profiles if p["inferred_type"] == "numerical"]
    for domain, keywords in DOMAIN_KEYWORDS.items():
        score = 0.0
        domain_signals: list[str] = []
        for keyword in keywords:
            if keyword in names:
                score += 2.5
                domain_signals.append(f"column keyword:{keyword}")
            if keyword in sample_blob:
                score += 0.8
                domain_signals.append(f"sample keyword:{keyword}")
            if keyword in lexical_tokens:
                score += 1.2
                domain_signals.append(f"nlp token:{keyword}")
            if keyword in sample_tokens:
                score += 0.5
                domain_signals.append(f"sample token:{keyword}")
        scores[domain] = score
        signals[domain] = domain_signals

    if {"revenue", "orders", "customer"} & set(role_map):
        scores["sales"] += 5.0
        signals["sales"].append("sales entity combination")
    if {"salary", "employee", "department"} & set(role_map):
        scores["hr"] += 5.0
        signals["hr"].append("hr entity combination")
    if {"revenue", "expense", "budget", "profit", "cash"} & set(role_map):
        scores["financial"] += 4.5
        signals["financial"].append("financial measure combination")
    if {"efficiency", "response_time", "throughput", "compliance"} & set(role_map):
        scores["operational"] += 4.0
        signals["operational"].append("operational metrics combination")
    if {"customer", "churn", "nps", "lifetime_value"} & set(role_map):
        scores["customer"] += 4.0
        signals["customer"].append("customer lifecycle combination")
    if {"inventory", "supplier"} & set(role_map):
        scores["inventory"] += 4.0
        signals["inventory"].append("inventory entity combination")
    if temporal_columns and numerical_columns:
        scores["time_series"] += 4.0
        signals["time_series"].append("temporal plus numerical structure")

    primary_domain = max(scores, key=scores.get) if scores else "operational"
    total = sum(max(score, 0.0) for score in scores.values()) or 1.0
    confidence = round(scores[primary_domain] / total, 2)
    secondary = [domain for domain, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)[1:3] if score > 0]
    return {
        "primary_domain": primary_domain,
        "confidence": confidence,
        "secondary_domains": secondary,
        "scores": {key: round(value, 2) for key, value in scores.items()},
        "signals": signals[primary_domain][:6],
    }


def _find_column(profiles: list[dict[str, Any]], keywords: list[str], inferred_type: str | None = None) -> str | None:
    for profile in profiles:
        lower_name = profile["name"].lower()
        if any(keyword in lower_name for keyword in keywords):
            if inferred_type is None or profile["inferred_type"] == inferred_type:
                return profile["name"]
    for profile in profiles:
        if inferred_type is None or profile["inferred_type"] == inferred_type:
            return profile["name"]
    return None


def _resolve_role_map(profiles: list[dict[str, Any]]) -> dict[str, str]:
    role_map: dict[str, str] = {}
    for role, config in ROLE_HINTS.items():
        column = _find_column(profiles, config["keywords"], config["type"])
        if column:
            role_map[role] = column
    return role_map


def _format_metric(value: Any, format_hint: str | None) -> str:
    if value is None:
        return "N/A"
    if format_hint == "currency":
        return f"${float(value):,.2f}"
    if format_hint == "percentage":
        return f"{float(value):.2f}%"
    if format_hint == "ratio":
        return f"{float(value):.2f}x"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.2f}"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    return str(value)


def _build_kpi(
    identifier: str,
    title: str,
    description: str,
    value: Any,
    format_hint: str | None,
    priority: int,
    business_impact: str,
    source_columns: list[str],
    recommended_visual: str,
    data_completeness: float = 100.0,
) -> dict[str, Any]:
    return {
        "id": identifier,
        "title": title,
        "description": description,
        "value": _format_metric(value, format_hint),
        "format_hint": format_hint,
        "priority": priority,
        "business_impact": business_impact,
        "source_columns": source_columns,
        "recommended_visual": recommended_visual,
        "data_completeness": data_completeness,
    }


def _completeness_for_columns(profiles: list[dict[str, Any]], columns: list[str]) -> float:
    if not columns:
        return 100.0
    missing_percentages = []
    for column in columns:
        profile = next((item for item in profiles if item["name"] == column), None)
        if profile:
            missing_percentages.append(profile["missing_percentage"])
    return round(100 - (sum(missing_percentages) / len(missing_percentages)), 2) if missing_percentages else 0.0


def _get_numeric_series(dataframe: pd.DataFrame, role_map: dict[str, str], *roles: str) -> tuple[str | None, pd.Series]:
    for role in roles:
        column = role_map.get(role)
        if column:
            return column, pd.to_numeric(dataframe[column], errors="coerce")
    return None, pd.Series(dtype=float)


def _get_date_series(dataframe: pd.DataFrame, role_map: dict[str, str]) -> tuple[str | None, pd.Series]:
    column = role_map.get("date")
    if not column:
        return None, pd.Series(dtype="datetime64[ns]")
    return column, pd.to_datetime(dataframe[column], errors="coerce")


def _growth_over_time(date_series: pd.Series, numeric_series: pd.Series) -> float | None:
    frame = pd.DataFrame({"date": date_series, "value": numeric_series}).dropna().sort_values("date")
    if len(frame) < 2:
        return None
    grouped = frame.groupby(frame["date"].dt.to_period("M"))["value"].sum().sort_index()
    if len(grouped) < 2 or grouped.iloc[0] == 0:
        return None
    return float(((grouped.iloc[-1] - grouped.iloc[0]) / abs(grouped.iloc[0])) * 100)


def _seasonality_index(date_series: pd.Series, numeric_series: pd.Series) -> float | None:
    frame = pd.DataFrame({"date": date_series, "value": numeric_series}).dropna().sort_values("date")
    if len(frame) < 6:
        return None
    grouped = frame.groupby(frame["date"].dt.month)["value"].mean()
    if grouped.empty or grouped.mean() == 0:
        return None
    return float(grouped.std(ddof=0) / grouped.mean())


def _volatility_index(numeric_series: pd.Series) -> float | None:
    clean = numeric_series.dropna()
    if len(clean) < 2 or clean.mean() == 0:
        return None
    return float((clean.std(ddof=0) / abs(clean.mean())) * 100)


def _detect_anomaly_counts(dataframe: pd.DataFrame, numeric_columns: list[str]) -> list[dict[str, Any]]:
    if not numeric_columns:
        return []
    frame = dataframe[numeric_columns].apply(pd.to_numeric, errors="coerce").dropna()
    if len(frame) < 10:
        return []
    records: list[dict[str, Any]] = []
    feature_count = max(1, min(len(numeric_columns), frame.shape[1]))
    if IForest is not None:
        model = IForest(contamination=0.1, random_state=42)
        model.fit(frame)
        predicted = model.predict(frame)
        anomaly_count = int(np.sum(predicted == 1))
        if anomaly_count:
            records.append(
                {
                    "method": "pyod_iforest",
                    "anomaly_count": anomaly_count,
                    "anomaly_percentage": round((anomaly_count / len(frame)) * 100, 2),
                    "features": numeric_columns[:feature_count],
                }
            )
    elif IsolationForest is not None:
        model = IsolationForest(contamination=0.1, random_state=42)
        predicted = model.fit_predict(frame)
        anomaly_count = int(np.sum(predicted == -1))
        if anomaly_count:
            records.append(
                {
                    "method": "sklearn_isolation_forest",
                    "anomaly_count": anomaly_count,
                    "anomaly_percentage": round((anomaly_count / len(frame)) * 100, 2),
                    "features": numeric_columns[:feature_count],
                }
            )
    return records


def _rate_from_series(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    numeric = pd.to_numeric(clean, errors="coerce")
    if numeric.notna().mean() >= 0.9:
        mean_value = float(numeric.dropna().mean())
        return mean_value * 100 if mean_value <= 1 else mean_value
    normalized = clean.astype(str).str.strip().str.lower()
    if normalized.isin({"true", "false", "yes", "no", "1", "0", "y", "n"}).any():
        return float(normalized.isin({"true", "yes", "1", "y"}).mean() * 100)
    return None


def _top_group_value(group_values: pd.Series, numeric_values: pd.Series) -> tuple[str, float] | None:
    frame = pd.DataFrame({"group": group_values, "value": numeric_values}).dropna()
    if frame.empty:
        return None
    grouped = frame.groupby("group")["value"].sum().sort_values(ascending=False)
    if grouped.empty:
        return None
    return str(grouped.index[0]), float(grouped.iloc[0])


def _recommend_kpis(dataframe: pd.DataFrame, profiles: list[dict[str, Any]], domain: dict[str, Any], role_map: dict[str, str]) -> list[dict[str, Any]]:
    domain_name = domain["primary_domain"]
    date_column, date_series = _get_date_series(dataframe, role_map)
    primary_value_column, primary_value_series = _get_numeric_series(
        dataframe,
        role_map,
        "revenue",
        "salary",
        "expense",
        "inventory",
        "throughput",
        "efficiency",
        "response_time",
        "nps",
    )
    recommendations: list[dict[str, Any]] = [
        _build_kpi(
            identifier="record_count",
            title="Total Records",
            description="Total number of rows available for analysis.",
            value=len(dataframe),
            format_hint=None,
            priority=1,
            business_impact="high",
            source_columns=[],
            recommended_visual="metric_card",
        )
    ]

    if primary_value_column and not primary_value_series.dropna().empty:
        recommendations.append(
            _build_kpi(
                identifier="primary_total",
                title=f"Total {primary_value_column.replace('_', ' ').title()}",
                description=f"Aggregate sum of {primary_value_column}.",
                value=float(primary_value_series.dropna().sum()),
                format_hint="currency" if any(token in primary_value_column.lower() for token in {"revenue", "sales", "income", "expense", "salary", "cost"}) else None,
                priority=1,
                business_impact="high",
                source_columns=[primary_value_column],
                recommended_visual="large_number_card",
                data_completeness=_completeness_for_columns(profiles, [primary_value_column]),
            )
        )

    if domain_name == "sales":
        revenue_column, revenue_series = _get_numeric_series(dataframe, role_map, "revenue")
        orders_column, orders_series = _get_numeric_series(dataframe, role_map, "orders")
        if revenue_column:
            recommendations.append(_build_kpi("total_revenue", "Total Revenue / Sales", "Total revenue generated.", float(revenue_series.dropna().sum()), "currency", 1, "high", [revenue_column], "large_number_card", _completeness_for_columns(profiles, [revenue_column])))
        if orders_column:
            recommendations.append(_build_kpi("order_count", "Number of Orders / Transactions", "Total transaction volume.", float(orders_series.dropna().sum()), None, 1, "high", [orders_column], "metric_card", _completeness_for_columns(profiles, [orders_column])))
            if revenue_column and orders_series.dropna().sum() > 0:
                recommendations.append(_build_kpi("average_order_value", "Average Order Value (AOV)", "Average revenue per order.", float(revenue_series.dropna().sum() / orders_series.dropna().sum()), "currency", 2, "high", [revenue_column, orders_column], "metric_card", _completeness_for_columns(profiles, [revenue_column, orders_column])))
        if role_map.get("product") and revenue_column:
            top_product = _top_group_value(dataframe[role_map["product"]], revenue_series)
            if top_product:
                recommendations.append(_build_kpi("top_selling_products", "Top Selling Products", "Best-performing product or category.", f"{top_product[0]} ({_format_metric(top_product[1], 'currency')})", None, 3, "medium", [role_map["product"], revenue_column], "ranked_list", _completeness_for_columns(profiles, [role_map["product"], revenue_column])))
        if date_column and revenue_column:
            growth = _growth_over_time(date_series, revenue_series)
            if growth is not None:
                recommendations.append(_build_kpi("sales_growth_rate", "Sales Growth Rate", "Growth of revenue across the observed period.", growth, "percentage", 2, "high", [date_column, revenue_column], "trend_metric_card", _completeness_for_columns(profiles, [date_column, revenue_column])))

    if domain_name == "hr":
        salary_column, salary_series = _get_numeric_series(dataframe, role_map, "salary")
        if salary_column:
            recommendations.append(_build_kpi("total_payroll", "Total Payroll / Average Salary", "Total payroll cost.", float(salary_series.dropna().sum()), "currency", 1, "high", [salary_column], "large_number_card", _completeness_for_columns(profiles, [salary_column])))
            recommendations.append(_build_kpi("average_salary", "Average Salary", "Average employee compensation.", float(salary_series.dropna().mean()), "currency", 1, "high", [salary_column], "metric_card", _completeness_for_columns(profiles, [salary_column])))
        if role_map.get("employee"):
            employee_column = role_map["employee"]
            recommendations.append(_build_kpi("headcount", "Headcount by Role/Department", "Distinct employee count.", int(dataframe[employee_column].nunique(dropna=True)), None, 1, "high", [employee_column], "metric_card", _completeness_for_columns(profiles, [employee_column])))
        if role_map.get("attrition"):
            attrition_rate = _rate_from_series(dataframe[role_map["attrition"]])
            if attrition_rate is not None:
                recommendations.append(_build_kpi("attrition_rate", "Attrition Rate", "Share of employees marked as attrited.", attrition_rate, "percentage", 2, "high", [role_map["attrition"]], "trend_metric_card", _completeness_for_columns(profiles, [role_map["attrition"]])))

    if domain_name == "financial":
        revenue_column, revenue_series = _get_numeric_series(dataframe, role_map, "revenue")
        expense_column, expense_series = _get_numeric_series(dataframe, role_map, "expense")
        if revenue_column:
            recommendations.append(_build_kpi("total_income", "Total Revenue / Income", "Total income across records.", float(revenue_series.dropna().sum()), "currency", 1, "high", [revenue_column], "large_number_card", _completeness_for_columns(profiles, [revenue_column])))
        if expense_column:
            recommendations.append(_build_kpi("total_expenses", "Total Expenses / Costs", "Total costs across records.", float(expense_series.dropna().sum()), "currency", 1, "high", [expense_column], "metric_card", _completeness_for_columns(profiles, [expense_column])))
        if revenue_column and expense_column:
            recommendations.append(_build_kpi("net_profit", "Net Profit / Margin", "Net profit after expenses.", float(revenue_series.dropna().sum() - expense_series.dropna().sum()), "currency", 1, "high", [revenue_column, expense_column], "metric_card", _completeness_for_columns(profiles, [revenue_column, expense_column])))

    if domain_name == "operational":
        for role, title in [("efficiency", "Efficiency Metrics"), ("throughput", "Throughput"), ("response_time", "Response Time"), ("compliance", "Compliance Rate")]:
            column = role_map.get(role)
            if not column:
                continue
            if role == "compliance":
                rate = _rate_from_series(dataframe[column])
                if rate is not None:
                    recommendations.append(_build_kpi("compliance_rate", title, "Operational compliance or quality score.", rate, "percentage", 2, "medium", [column], "metric_card", _completeness_for_columns(profiles, [column])))
            else:
                numeric = pd.to_numeric(dataframe[column], errors="coerce").dropna()
                if not numeric.empty:
                    recommendations.append(_build_kpi(f"{role}_metric", title, f"Primary {title.lower()} signal from the uploaded data.", float(numeric.mean() if role in {'efficiency', 'response_time'} else numeric.sum()), "percentage" if role == "efficiency" and numeric.max() <= 1 else None, 1 if role != "response_time" else 2, "high", [column], "metric_card", _completeness_for_columns(profiles, [column])))

    if domain_name == "customer":
        if role_map.get("customer"):
            customer_column = role_map["customer"]
            recommendations.append(_build_kpi("total_customers", "Total Customers / Segments", "Distinct customers represented in the dataset.", int(dataframe[customer_column].nunique(dropna=True)), None, 1, "high", [customer_column], "large_number_card", _completeness_for_columns(profiles, [customer_column])))
        if role_map.get("churn"):
            churn_rate = _rate_from_series(dataframe[role_map["churn"]])
            if churn_rate is not None:
                recommendations.append(_build_kpi("customer_churn_rate", "Customer Churn Rate", "Share of churned customers.", churn_rate, "percentage", 1, "high", [role_map["churn"]], "trend_metric_card", _completeness_for_columns(profiles, [role_map["churn"]])))
        if role_map.get("lifetime_value"):
            ltv_column, ltv_series = _get_numeric_series(dataframe, role_map, "lifetime_value")
            recommendations.append(_build_kpi("customer_lifetime_value", "Customer Lifetime Value", "Average long-term customer value.", float(ltv_series.dropna().mean()), "currency", 2, "high", [ltv_column] if ltv_column else [], "metric_card", _completeness_for_columns(profiles, [ltv_column] if ltv_column else [])))

    if domain_name == "inventory":
        inventory_column, inventory_series = _get_numeric_series(dataframe, role_map, "inventory")
        if inventory_column:
            recommendations.append(_build_kpi("total_inventory", "Total Inventory", "Total stock available.", float(inventory_series.dropna().sum()), None, 1, "high", [inventory_column], "large_number_card", _completeness_for_columns(profiles, [inventory_column])))
            volatility = _volatility_index(inventory_series)
            if volatility is not None:
                recommendations.append(_build_kpi("inventory_volatility", "Demand / Stock Volatility", "Relative variability in stock levels.", volatility, "percentage", 2, "medium", [inventory_column], "metric_card", _completeness_for_columns(profiles, [inventory_column])))

    if domain_name == "time_series" and date_column and primary_value_column:
        growth = _growth_over_time(date_series, primary_value_series)
        seasonality = _seasonality_index(date_series, primary_value_series)
        volatility = _volatility_index(primary_value_series)
        if growth is not None:
            recommendations.append(_build_kpi("trend_growth_rate", "Trend Line / Growth Rate", "Directional change across the time series.", growth, "percentage", 1, "high", [date_column, primary_value_column], "trend_metric_card", _completeness_for_columns(profiles, [date_column, primary_value_column])))
        if seasonality is not None:
            recommendations.append(_build_kpi("seasonality_index", "Seasonality Index", "Strength of repeating monthly patterns.", seasonality, "ratio", 2, "medium", [date_column, primary_value_column], "scorecard", _completeness_for_columns(profiles, [date_column, primary_value_column])))
        if volatility is not None:
            recommendations.append(_build_kpi("volatility", "Volatility / Standard Deviation", "Relative variation of the primary time-series metric.", volatility, "percentage", 2, "medium", [primary_value_column], "metric_card", _completeness_for_columns(profiles, [primary_value_column])))

    recommendations.append(
        _build_kpi(
            identifier="data_completeness",
            title="Data Completeness",
            description="Overall non-missing value percentage across the dataset.",
            value=100 - round(float(dataframe.isna().mean().mean() * 100), 2),
            format_hint="percentage",
            priority=4,
            business_impact="medium",
            source_columns=list(dataframe.columns),
            recommended_visual="scorecard",
        )
    )

    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in sorted(recommendations, key=lambda entry: (entry["priority"], -entry["data_completeness"])):
        if item["id"] in seen_ids or item["value"] in {"N/A", "None"}:
            continue
        seen_ids.add(item["id"])
        deduped.append(item)
        if len(deduped) >= 8:
            break
    return deduped


def _recommend_charts(profiles: list[dict[str, Any]], domain: dict[str, Any], role_map: dict[str, str]) -> list[dict[str, Any]]:
    charts: list[dict[str, Any]] = []
    date_column = role_map.get("date")
    primary_value_column = role_map.get("revenue") or role_map.get("salary") or role_map.get("expense") or role_map.get("inventory") or role_map.get("throughput")
    category_column = role_map.get("product") or role_map.get("department") or role_map.get("region") or role_map.get("supplier")
    numeric_columns = [profile["name"] for profile in profiles if profile["inferred_type"] == "numerical"]

    if domain["primary_domain"] == "sales":
        if date_column and role_map.get("revenue"):
            charts.append({"id": "sales_trend", "title": "Monthly Trend", "chart_type": "line", "x_field": date_column, "y_field": role_map["revenue"], "aggregation": "sum", "rationale": "Revenue trend over time is the primary sales signal."})
        if role_map.get("product") and role_map.get("revenue"):
            charts.append({"id": "top_products", "title": "Top Selling Products", "chart_type": "horizontal_bar", "x_field": role_map["product"], "y_field": role_map["revenue"], "aggregation": "sum", "rationale": "Ranked product views show where revenue concentrates."})
        if role_map.get("region") and role_map.get("revenue"):
            charts.append({"id": "revenue_by_region", "title": "Revenue by Region", "chart_type": "map", "x_field": role_map["region"], "y_field": role_map["revenue"], "aggregation": "sum", "rationale": "Geographic sales performance is easier to interpret on a map."})

    if domain["primary_domain"] == "hr":
        if role_map.get("salary"):
            charts.append({"id": "salary_distribution", "title": "Salary Distribution", "chart_type": "histogram", "x_field": role_map["salary"], "y_field": None, "aggregation": None, "rationale": "Salary distributions reveal pay-band skew and outliers."})
        if role_map.get("department") and role_map.get("employee"):
            charts.append({"id": "headcount_by_department", "title": "Headcount by Department", "chart_type": "grouped_bar", "x_field": role_map["department"], "y_field": role_map["employee"], "aggregation": "nunique", "rationale": "Department headcount is a core workforce planning view."})

    if domain["primary_domain"] == "financial":
        if role_map.get("revenue") and role_map.get("expense"):
            charts.append({"id": "revenue_vs_expenses", "title": "Revenue vs Expenses", "chart_type": "stacked_column", "x_field": date_column, "y_field": None, "aggregation": "sum", "series": [role_map["revenue"], role_map["expense"]], "rationale": "Placing revenue and expense together shows profitability pressure clearly."})
        if role_map.get("budget") and role_map.get("revenue"):
            charts.append({"id": "budget_vs_actual", "title": "Budget vs Actual", "chart_type": "grouped_bar", "x_field": date_column, "y_field": None, "aggregation": "sum", "series": [role_map["budget"], role_map["revenue"]], "rationale": "Budget comparisons should be shown directly against actuals."})

    if domain["primary_domain"] == "customer":
        if date_column and role_map.get("churn"):
            charts.append({"id": "churn_rate_trend", "title": "Churn Rate Trend", "chart_type": "line", "x_field": date_column, "y_field": role_map["churn"], "aggregation": "mean", "rationale": "Churn trend is the core customer-health time series."})
        if role_map.get("region") and role_map.get("customer"):
            charts.append({"id": "customer_segments", "title": "Customer Segments", "chart_type": "donut", "x_field": role_map["region"], "y_field": role_map["customer"], "aggregation": "nunique", "rationale": "Segment composition works well as a part-to-whole visual."})

    if domain["primary_domain"] == "time_series" and date_column and primary_value_column:
        charts.append({"id": "trend_with_forecast", "title": "Trend Over Time", "chart_type": "line_with_forecast", "x_field": date_column, "y_field": primary_value_column, "aggregation": "sum", "rationale": "Time-series dashboards should start with trend plus forecast context."})
        charts.append({"id": "seasonality_pattern", "title": "Seasonality Pattern", "chart_type": "heatmap", "x_field": date_column, "y_field": primary_value_column, "aggregation": "sum", "rationale": "Heat maps surface recurring temporal patterns."})

    if date_column and primary_value_column:
        charts.append({"id": "trend_over_time", "title": f"{primary_value_column.replace('_', ' ').title()} Over Time", "chart_type": "line", "x_field": date_column, "y_field": primary_value_column, "aggregation": "sum", "rationale": "Temporal numeric data should expose trend and change over time."})
    if category_column and primary_value_column:
        charts.append({"id": "category_breakdown", "title": f"{primary_value_column.replace('_', ' ').title()} by {category_column.replace('_', ' ').title()}", "chart_type": "bar", "x_field": category_column, "y_field": primary_value_column, "aggregation": "sum", "rationale": "Bar charts compare categories clearly."})
    if category_column:
        charts.append({"id": "category_share", "title": f"{category_column.replace('_', ' ').title()} Share", "chart_type": "donut", "x_field": category_column, "y_field": None, "aggregation": "count", "rationale": "Low-cardinality categories are effective as share visuals."})
    if numeric_columns:
        charts.append({"id": "distribution", "title": f"{numeric_columns[0].replace('_', ' ').title()} Distribution", "chart_type": "histogram", "x_field": numeric_columns[0], "y_field": None, "aggregation": None, "rationale": "Distribution views reveal skew and outliers."})
    if len(numeric_columns) >= 2:
        charts.append({"id": "correlation", "title": f"{numeric_columns[0].replace('_', ' ').title()} vs {numeric_columns[1].replace('_', ' ').title()}", "chart_type": "scatter", "x_field": numeric_columns[0], "y_field": numeric_columns[1], "aggregation": None, "rationale": "Scatter plots expose measure relationships."})

    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for chart in charts:
        if chart["id"] in seen_ids:
            continue
        seen_ids.add(chart["id"])
        deduped.append(chart)
        if len(deduped) >= 6:
            break
    return deduped


def _build_dashboard_blueprint(profiles: list[dict[str, Any]], kpis: list[dict[str, Any]], charts: list[dict[str, Any]], domain: dict[str, Any], role_map: dict[str, str]) -> dict[str, Any]:
    theme = DOMAIN_THEMES.get(domain["primary_domain"], DOMAIN_THEMES["operational"])
    filter_candidates = [
        {"field": profile["name"], "type": "select" if profile["inferred_type"] in {"categorical", "geolocation"} else "date_range", "label": profile["name"].replace("_", " ").title()}
        for profile in profiles
        if ((profile["inferred_type"] in {"categorical", "geolocation"} and profile["cardinality"] in {"low", "medium"}) or profile["inferred_type"] == "temporal")
    ][:5]

    components: list[dict[str, Any]] = []
    for index, kpi in enumerate(kpis[:4]):
        components.append({"id": kpi["id"], "kind": "kpi_card", "title": kpi["title"], "size": "small" if index < 3 else "medium", "layout": {"grid_columns": 12, "x": (index % 4) * 3, "y": 0, "w": 3, "h": 2}, "config": {"format_hint": kpi["format_hint"], "source_columns": kpi["source_columns"], "trend": "sparkline" if "trend" in kpi["recommended_visual"] else None}})
    for index, chart in enumerate(charts[:4]):
        components.append({"id": chart["id"], "kind": chart["chart_type"], "title": chart["title"], "size": "large", "layout": {"grid_columns": 12, "x": 0 if index % 2 == 0 else 6, "y": 2 + (index // 2) * 4, "w": 6, "h": 4}, "config": chart})
    components.append({"id": "data_preview", "kind": "table", "title": "Data Preview", "size": "full-width", "layout": {"grid_columns": 12, "x": 0, "y": 10, "w": 12, "h": 4}, "config": {"columns": [profile["name"] for profile in profiles[:12]]}})
    return {
        "layout_system": "12-column grid",
        "grid_columns": 12,
        "card_sizing": {"kpi": "small", "charts": "large", "table": "full-width"},
        "filters": filter_candidates,
        "theme": theme,
        "typography": {"heading_scale": "display-sm", "body_scale": "text-sm", "font_stack": "Inter, system-ui, sans-serif"},
        "spacing": {"section_gap": 24, "card_gap": 16},
        "interactive_elements": ["date_range_selector", "category_filters", "drill_down", "cross_filtering"],
        "layout_guidance": {"summary_band": "Place top-level metrics first.", "analysis_band": "Show trends and segment analytics next.", "detail_band": "Keep preview tables and exports below charts."},
        "domain_context": {"primary_domain": domain["primary_domain"], "recommended_focus_fields": [value for key, value in role_map.items() if key in {"date", "revenue", "salary", "expense", "customer", "product", "region"}]},
        "components": components,
    }


def _data_quality_report(dataframe: pd.DataFrame, profiles: list[dict[str, Any]]) -> dict[str, Any]:
    duplicate_rows = int(dataframe.duplicated().sum())
    missing_average = round(float(dataframe.isna().mean().mean() * 100), 2)
    high_missing_columns = [profile["name"] for profile in profiles if profile["missing_percentage"] >= 20]
    anomaly_columns = [
        {"column": profile["name"], "outlier_count": profile["stats"].get("outlier_count", 0)}
        for profile in profiles
        if profile["inferred_type"] == "numerical" and profile["stats"].get("outlier_count", 0) > 0
    ][:6]
    recommendations = []
    if duplicate_rows:
        recommendations.append("Review duplicate rows before publishing KPI totals.")
    if high_missing_columns:
        recommendations.append("Consider imputing or excluding columns with high missingness.")
    if anomaly_columns:
        recommendations.append("Validate outliers before using the dataset for forecasts or benchmarks.")
    return {
        "duplicate_rows": duplicate_rows,
        "duplicate_percentage": round((duplicate_rows / len(dataframe)) * 100, 2) if len(dataframe) else 0.0,
        "average_missing_percentage": missing_average,
        "high_missing_columns": high_missing_columns,
        "anomaly_columns": anomaly_columns,
        "completeness_score": round(100 - missing_average, 2),
        "recommended_actions": recommendations,
    }


def _statistical_summary(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        profile["name"]: profile["stats"]
        for profile in profiles
        if profile["stats"]
    }


def _column_headers(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "column_name": profile["name"],
            "data_type": profile["inferred_type"],
            "subtype": profile["subtype"],
            "missing_percentage": profile["missing_percentage"],
            "cardinality": profile["cardinality"],
        }
        for profile in profiles
    ]


def _column_statistics(profiles: list[dict[str, Any]], row_count: int) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for profile in profiles:
        entry = {
            "data_type": profile["inferred_type"],
            "subtype": profile["subtype"],
            "unique_count": profile["unique_count"],
            "missing_percentage": profile["missing_percentage"],
            "non_null_percentage": round(100 - profile["missing_percentage"], 2),
            "uniqueness_percentage": round((profile["unique_count"] / row_count) * 100, 2) if row_count else 0.0,
            "cardinality": profile["cardinality"],
            **profile["stats"],
        }
        stats[profile["name"]] = entry
    return stats


def _missing_data_heatmap(dataframe: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    heatmap: list[dict[str, Any]] = []
    preview = dataframe.head(limit)
    for row_index, row in preview.iterrows():
        heatmap.append(
            {
                "row_index": int(row_index),
                "missing": {column: _is_missing_value(value) for column, value in row.items()},
            }
        )
    return heatmap


def _mini_histograms(dataframe: pd.DataFrame, profiles: list[dict[str, Any]]) -> dict[str, Any]:
    histograms: dict[str, Any] = {}
    for profile in profiles:
        if profile["inferred_type"] != "numerical":
            continue
        series = pd.to_numeric(dataframe[profile["name"]], errors="coerce").dropna()
        if series.empty:
            continue
        counts, bin_edges = np.histogram(series, bins=min(8, max(3, len(series.unique()))))
        histograms[profile["name"]] = {
            "bins": [round(float(edge), 4) for edge in bin_edges.tolist()],
            "counts": [int(count) for count in counts.tolist()],
        }
    return histograms


def _correlation_matrix(dataframe: pd.DataFrame, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    numeric_columns = [profile["name"] for profile in profiles if profile["inferred_type"] == "numerical"]
    if len(numeric_columns) < 2:
        return []
    frame = dataframe[numeric_columns].apply(pd.to_numeric, errors="coerce")
    matrix = frame.corr(numeric_only=True)
    return [
        {"row": row, "column": column, "value": round(float(matrix.loc[row, column]), 3)}
        for row in matrix.index
        for column in matrix.columns
        if pd.notna(matrix.loc[row, column])
    ]


def _value_frequency_distribution(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    frequencies: dict[str, Any] = {}
    for profile in profiles:
        top_values = profile["stats"].get("top_values")
        if top_values:
            frequencies[profile["name"]] = top_values
    return frequencies


def _materialize_chart_data(dataframe: pd.DataFrame, charts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    for chart in charts:
        resolved = dict(chart)
        chart_type = chart.get("chart_type")
        x_field = chart.get("x_field")
        y_field = chart.get("y_field")
        aggregation = chart.get("aggregation")
        data: list[dict[str, Any]] = []
        frontend_type = "bar"

        try:
            if chart_type in {"line", "line_with_forecast"} and x_field and y_field:
                frame = pd.DataFrame(
                    {
                        "x": pd.to_datetime(dataframe[x_field], errors="coerce"),
                        "y": pd.to_numeric(dataframe[y_field], errors="coerce"),
                    }
                ).dropna()
                if not frame.empty:
                    grouped = frame.groupby(frame["x"].dt.to_period("M"))["y"].sum().sort_index()
                    data = [{"label": str(index), "value": round(float(value), 2)} for index, value in grouped.items()]
                frontend_type = "line" if chart_type == "line" else "area"
            elif chart_type in {"bar", "horizontal_bar", "grouped_bar", "map"} and x_field and y_field:
                frame = pd.DataFrame(
                    {"x": dataframe[x_field].astype(str), "y": pd.to_numeric(dataframe[y_field], errors="coerce")}
                ).dropna()
                if not frame.empty:
                    grouped = frame.groupby("x")["y"].sum().sort_values(ascending=False).head(8)
                    data = [{"label": str(index), "value": round(float(value), 2)} for index, value in grouped.items()]
                frontend_type = "bar"
            elif chart_type == "donut" and x_field:
                if aggregation == "nunique" and y_field:
                    frame = pd.DataFrame({"x": dataframe[x_field].astype(str), "y": dataframe[y_field].astype(str)}).dropna()
                    grouped = frame.groupby("x")["y"].nunique().sort_values(ascending=False).head(8)
                    data = [{"label": str(index), "value": int(value)} for index, value in grouped.items()]
                else:
                    grouped = dataframe[x_field].astype(str).value_counts().head(8)
                    data = [{"label": str(index), "value": int(value)} for index, value in grouped.items()]
                frontend_type = "pie"
            elif chart_type == "histogram" and x_field:
                numeric = pd.to_numeric(dataframe[x_field], errors="coerce").dropna()
                if not numeric.empty:
                    counts, bins = np.histogram(numeric, bins=min(8, max(4, len(numeric.unique()))))
                    data = [
                        {"label": f"{round(float(bins[i]), 2)}-{round(float(bins[i + 1]), 2)}", "value": int(counts[i])}
                        for i in range(len(counts))
                    ]
                frontend_type = "bar"
            elif chart_type == "scatter" and x_field and y_field:
                frame = pd.DataFrame(
                    {"x": pd.to_numeric(dataframe[x_field], errors="coerce"), "y": pd.to_numeric(dataframe[y_field], errors="coerce")}
                ).dropna()
                if not frame.empty:
                    data = [{"label": round(float(row.x), 2), "value": round(float(row.y), 2)} for row in frame.head(50).itertuples()]
                frontend_type = "line"
            elif chart_type in {"stacked_column"} and x_field and chart.get("series"):
                frame = pd.DataFrame({"x": pd.to_datetime(dataframe[x_field], errors="coerce")})
                for series_name in chart["series"]:
                    frame[series_name] = pd.to_numeric(dataframe[series_name], errors="coerce")
                frame = frame.dropna()
                if not frame.empty:
                    grouped = frame.groupby(frame["x"].dt.to_period("M"))[chart["series"]].sum().sort_index().reset_index()
                    data = [
                        {"label": str(row["x"]), **{series_name: round(float(row[series_name]), 2) for series_name in chart["series"]}}
                        for _, row in grouped.iterrows()
                    ]
                frontend_type = "bar"
        except Exception:
            data = []

        resolved["frontend_type"] = frontend_type
        resolved["data"] = data
        resolved["x_key"] = "label"
        resolved["y_key"] = "value" if all("value" in row for row in data) else (chart.get("series", ["value"])[0] if chart.get("series") else "value")
        materialized.append(resolved)
    return materialized


def _statistical_analysis(dataframe: pd.DataFrame, profiles: list[dict[str, Any]], role_map: dict[str, str]) -> dict[str, Any]:
    analysis: dict[str, Any] = {
        "correlations": [],
        "anomaly_signals": [],
        "model_detected_anomalies": [],
        "time_series_analysis": {},
        "segmentation_signals": [],
    }
    numeric_columns = [profile["name"] for profile in profiles if profile["inferred_type"] == "numerical"]
    if len(numeric_columns) >= 2:
        numeric_frame = dataframe[numeric_columns].apply(pd.to_numeric, errors="coerce")
        correlation_matrix = numeric_frame.corr(numeric_only=True)
        pairs: list[dict[str, Any]] = []
        for idx, left in enumerate(correlation_matrix.columns):
            for right in correlation_matrix.columns[idx + 1 :]:
                corr = correlation_matrix.loc[left, right]
                if pd.notna(corr) and abs(float(corr)) >= 0.5:
                    pairs.append({"left": left, "right": right, "correlation": round(float(corr), 3)})
        analysis["correlations"] = sorted(pairs, key=lambda item: abs(item["correlation"]), reverse=True)[:6]

    for profile in profiles:
        if profile["inferred_type"] == "numerical" and profile["stats"].get("outlier_count", 0) > 0:
            analysis["anomaly_signals"].append({"column": profile["name"], "outlier_count": profile["stats"]["outlier_count"], "distribution": profile["stats"].get("distribution")})
    analysis["model_detected_anomalies"] = _detect_anomaly_counts(dataframe, numeric_columns[:8])

    date_column, date_series = _get_date_series(dataframe, role_map)
    value_column, value_series = _get_numeric_series(dataframe, role_map, "revenue", "salary", "expense", "inventory", "throughput")
    if date_column and value_column:
        frame = pd.DataFrame({"date": date_series, "value": value_series}).dropna().sort_values("date")
        if len(frame) >= 3:
            trend_strength = float(pd.Series(np.arange(len(frame))).corr(frame["value"])) if frame["value"].nunique() > 1 else 0.0
            autocorrelation = None
            if acf is not None and len(frame) >= 6:
                autocorr_values = acf(frame["value"], nlags=min(5, len(frame) - 1), fft=False)
                if len(autocorr_values) > 1:
                    autocorrelation = round(float(autocorr_values[1]), 3)
            analysis["time_series_analysis"] = {
                "primary_metric": value_column,
                "frequency": _detect_frequency(frame["date"]),
                "trend_strength": round(trend_strength, 3) if not math.isnan(trend_strength) else 0.0,
                "seasonality_index": round(_seasonality_index(frame["date"], frame["value"]) or 0.0, 3),
                "volatility_index": round(_volatility_index(frame["value"]) or 0.0, 3),
                "lag_1_autocorrelation": autocorrelation,
            }

    group_column = role_map.get("region") or role_map.get("department") or role_map.get("product")
    if group_column and value_column:
        grouped = (
            pd.DataFrame({"group": dataframe[group_column], "value": pd.to_numeric(dataframe[value_column], errors="coerce")})
            .dropna()
            .groupby("group")["value"]
            .sum()
            .sort_values(ascending=False)
        )
        analysis["segmentation_signals"] = [{"segment": str(label), "value": round(float(value), 2)} for label, value in grouped.head(5).items()]
    return analysis


def _narrative_insights(
    dataframe: pd.DataFrame,
    profiles: list[dict[str, Any]],
    domain: dict[str, Any],
    quality_report: dict[str, Any],
    kpis: list[dict[str, Any]],
    statistical_analysis: dict[str, Any],
) -> list[str]:
    insights = [
        f"The dataset is primarily classified as {domain['primary_domain']} data with {int(domain['confidence'] * 100)}% confidence.",
        f"The upload contains {len(dataframe):,} rows and {len(dataframe.columns)} columns.",
        f"Overall data completeness is {quality_report['completeness_score']}% with {quality_report['duplicate_rows']} duplicate rows detected.",
    ]
    if quality_report["high_missing_columns"]:
        insights.append("Columns with the highest missingness: " + ", ".join(quality_report["high_missing_columns"][:3]) + ".")
    if kpis:
        insights.append(f"The top recommended KPI is {kpis[0]['title']} with a current value of {kpis[0]['value']}.")
    if statistical_analysis.get("correlations"):
        strongest = statistical_analysis["correlations"][0]
        insights.append(f"The strongest numerical relationship is between {strongest['left']} and {strongest['right']} (corr {strongest['correlation']}).")
    if statistical_analysis.get("time_series_analysis", {}).get("frequency"):
        insights.append(f"The dataset behaves like {statistical_analysis['time_series_analysis']['frequency']} time-series data.")
    return insights[:6]


def _structured_ai_insights(
    dataframe: pd.DataFrame,
    profiles: list[dict[str, Any]],
    domain: dict[str, Any],
    quality_report: dict[str, Any],
    kpis: list[dict[str, Any]],
    statistical_analysis: dict[str, Any],
) -> dict[str, Any]:
    findings = _narrative_insights(dataframe, profiles, domain, quality_report, kpis, statistical_analysis)[:5]
    anomalies = [
        f"{signal['column']} has {signal['outlier_count']} detected outliers with a {signal.get('distribution', 'non-normal')} distribution."
        for signal in statistical_analysis.get("anomaly_signals", [])[:5]
    ]
    if statistical_analysis.get("model_detected_anomalies"):
        model_anomaly = statistical_analysis["model_detected_anomalies"][0]
        anomalies.append(
            f"{model_anomaly['method']} flagged {model_anomaly['anomaly_count']} anomalous rows across {', '.join(model_anomaly['features'])}."
        )
    trends: list[str] = []
    time_series = statistical_analysis.get("time_series_analysis", {})
    if time_series.get("frequency"):
        trends.append(
            f"{time_series['primary_metric']} shows {time_series['frequency']} behavior with trend strength {time_series.get('trend_strength', 0.0)}."
        )
    if time_series.get("seasonality_index", 0) > 0.15:
        trends.append(f"Seasonality is present with an index of {time_series['seasonality_index']}.")
    opportunities: list[str] = []
    if kpis:
        opportunities.append(f"Start the dashboard with {kpis[0]['title']} as the executive KPI.")
    if statistical_analysis.get("segmentation_signals"):
        top_segment = statistical_analysis["segmentation_signals"][0]
        opportunities.append(f"Focus on {top_segment['segment']} because it leads the tracked value at {top_segment['value']}.")
    risks: list[str] = []
    if quality_report["high_missing_columns"]:
        risks.append("High missingness may weaken KPI reliability for: " + ", ".join(quality_report["high_missing_columns"][:3]) + ".")
    if quality_report["duplicate_rows"]:
        risks.append(f"{quality_report['duplicate_rows']} duplicate rows should be resolved before publishing metrics.")
    if statistical_analysis.get("model_detected_anomalies"):
        risks.append("Machine-detected anomalies should be reviewed before using the dataset for forecasting or alerting.")
    correlations = [
        f"{item['left']} and {item['right']} are correlated at {item['correlation']}."
        for item in statistical_analysis.get("correlations", [])[:5]
    ]
    recommendations = list(quality_report.get("recommended_actions", []))
    if not recommendations:
        recommendations.append("Proceed to dashboard generation and validate metric definitions with business stakeholders.")
    return {
        "key_findings": findings,
        "anomalies": anomalies,
        "trends": trends,
        "opportunities": opportunities,
        "risks": risks,
        "correlations": correlations,
        "recommendations": recommendations[:5],
    }


def _preview_rows(dataframe: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    return [
        {column: _safe_value(value) for column, value in row.items()}
        for row in dataframe.head(limit).to_dict(orient="records")
    ]


def build_analysis_payload(dataframe: pd.DataFrame) -> dict[str, Any]:
    """Build the stored analysis payload for one dataset."""

    profiles = _profile_columns(dataframe)
    row_count = int(len(dataframe))
    role_map = _resolve_role_map(profiles)
    domain = _detect_domain(dataframe, profiles, role_map)
    relationships = _infer_relationships(dataframe, profiles, len(dataframe))
    quality_report = _data_quality_report(dataframe, profiles)
    statistical_analysis = _statistical_analysis(dataframe, profiles, role_map)
    kpis = _recommend_kpis(dataframe, profiles, domain, role_map)
    charts = _materialize_chart_data(dataframe, _recommend_charts(profiles, domain, role_map))
    blueprint = _build_dashboard_blueprint(profiles, kpis, charts, domain, role_map)
    preview = {
        "sample_rows": _preview_rows(dataframe),
        "column_headers": _column_headers(profiles),
        "column_statistics": _column_statistics(profiles, row_count),
        "data_quality_score": round(quality_report["completeness_score"] / 100, 4),
        "completeness": round(1 - (dataframe.isna().mean().mean()), 4) if row_count else 0.0,
        "data_quality_metrics": quality_report,
        "missing_data_heatmap": _missing_data_heatmap(dataframe),
        "mini_histograms": _mini_histograms(dataframe, profiles),
        "correlation_matrix": _correlation_matrix(dataframe, profiles),
        "value_frequency_distribution": _value_frequency_distribution(profiles),
        "statistical_summary": _statistical_summary(profiles),
    }
    ai_insights = _structured_ai_insights(dataframe, profiles, domain, quality_report, kpis, statistical_analysis)
    return {
        "schema_mapping": {
            "columns": profiles,
            "relationships": relationships,
            "role_mapping": role_map,
        },
        "domain_detection": domain,
        "quality_report": quality_report,
        "statistical_analysis": statistical_analysis,
        "recommended_kpis": kpis,
        "chart_recommendations": charts,
        "dashboard_blueprint": blueprint,
        "data_preview": preview,
        "insights": ai_insights["key_findings"],
        "ai_insights": ai_insights,
    }


async def analyze_dataset_upload(
    session: AsyncSession,
    *,
    actor: User,
    project_id: int,
    upload: UploadFile,
    ip_address: str | None,
) -> DatasetAsset:
    """Persist an uploaded dataset and generated analysis payload."""

    await get_project_or_404(session, project_id=project_id, user=actor, require_write=True)
    saved_path, sniff_bytes, total_size = await save_upload(upload, "datasets")
    dataframe, encoding = await asyncio.to_thread(_load_dataframe, saved_path, sniff_bytes)
    analysis_payload = await asyncio.to_thread(build_analysis_payload, dataframe)
    dataset = DatasetAsset(
        project_id=project_id,
        uploaded_by_id=actor.id,
        original_filename=upload.filename or saved_path.name,
        file_format=saved_path.suffix.lower().lstrip("."),
        encoding=encoding,
        file_size_bytes=total_size,
        stored_path=str(saved_path),
        row_count=int(len(dataframe)),
        column_count=int(len(dataframe.columns)),
        detected_domain=analysis_payload["domain_detection"]["primary_domain"],
        analysis_payload=analysis_payload,
    )
    session.add(dataset)
    await session.flush()
    await log_audit_event(
        session,
        action="dataset.analyzed",
        resource_type="dataset",
        resource_id=str(dataset.id),
        user_id=actor.id,
        ip_address=ip_address,
        payload={
            "project_id": project_id,
            "filename": dataset.original_filename,
            "domain": dataset.detected_domain,
        },
    )
    await session.commit()
    await session.refresh(dataset)
    return dataset
