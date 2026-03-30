"""Clustering and segmentation service."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, KMeans
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score,
)
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

from app.services.monitoring import (
    timed_operation,
    log_analysis_start,
    log_analysis_complete,
)
from app.services.validation import PreparedDataset, prepare_analysis_dataset
from app.services.error_handling import ModelTrainingError


@timed_operation("advanced_clustering", target_ms=30000)
def run_clustering(
    df: pd.DataFrame,
    *,
    dataset_id: str | int | None = None,
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Cluster numeric features into interpretable segments."""

    options = options or {}
    prepared: PreparedDataset = prepare_analysis_dataset(
        df,
        analysis_type="clustering",
        dataset_id=dataset_id,
        options=options,
        correlation_id=correlation_id,
    )
    feature_columns = prepared.plan["feature_columns"]
    working = prepared.dataframe[feature_columns].copy()
    scaler = StandardScaler()
    scaled = scaler.fit_transform(working)
    max_clusters = min(int(options.get("max_clusters", 6)), max(2, len(working) - 1))
    labels = None
    best_score = -1.0
    best_k = 2
    warnings = list(prepared.warnings)

    for cluster_count in range(2, max_clusters + 1):
        model = KMeans(n_clusters=cluster_count, random_state=42, n_init=10)
        candidate = model.fit_predict(scaled)
        unique_labels = np.unique(candidate)
        if len(unique_labels) < 2:
            continue
        score = silhouette_score(scaled, candidate)
        if score > best_score:
            best_score = float(score)
            best_k = cluster_count
            labels = candidate

    algorithm = "kmeans"
    if labels is None:
        model = DBSCAN(eps=1.2, min_samples=5)
        labels = model.fit_predict(scaled)
        algorithm = "dbscan"
        unique_labels = [label for label in np.unique(labels) if label != -1]
        best_k = len(unique_labels) or 1
        best_score = 0.0
        warnings.append(
            "KMeans did not produce a stable segmentation; DBSCAN fallback used."
        )

    working = working.copy()
    working["cluster_id"] = labels.astype(int)
    cluster_profiles = (
        working.groupby("cluster_id")[feature_columns]
        .mean(numeric_only=True)
        .round(4)
        .reset_index()
        .to_dict(orient="records")
    )
    sizes = working["cluster_id"].value_counts().sort_index()
    segment_summaries = [
        {
            "cluster_id": int(cluster_id),
            "size": int(sizes.loc[cluster_id]),
            "share": round(float(sizes.loc[cluster_id] / len(working)), 4),
            "type": "outlier" if int(cluster_id) == -1 else "segment",
        }
        for cluster_id in sizes.index.tolist()
    ]
    assignment_sample = (
        working.head(200)
        .reset_index()
        .rename(columns={"index": "row_index"})
        .to_dict(orient="records")
    )

    confidence = (
        "HIGH" if best_score >= 0.4 else ("MEDIUM" if best_score >= 0.2 else "LOW")
    )
    return {
        "status": "SUCCESS",
        "confidence": confidence,
        "analysis_type": "clustering",
        "processed_rows": int(len(prepared.dataframe)),
        "total_rows": int(len(df)),
        "excluded_rows": prepared.excluded_rows,
        "exclusion_reasons": {"preprocessing": prepared.excluded_rows},
        "quality_score": prepared.validation["quality_metrics"]["overall_score"],
        "validation": prepared.validation,
        "missing_values_analysis": prepared.missing_values_analysis,
        "results": {
            "algorithm": algorithm,
            "cluster_count": int(best_k),
            "silhouette_score": round(float(best_score), 4),
            "feature_columns": feature_columns,
            "cluster_profiles": cluster_profiles,
            "segment_summaries": segment_summaries,
            "assignment_sample": assignment_sample,
        },
        "warnings": warnings,
    }


class ClusteringEngine:
    """Clustering and segmentation engine"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df.copy()
        self.dataset_id = dataset_id
        self.scaler = StandardScaler()
        self.best_model = None
        self.best_algorithm = None

    @timed_operation("Clustering Analysis")
    def cluster(
        self,
        n_clusters: Optional[int] = None,
        max_clusters: int = 10,
        algorithm: str = "auto",
    ) -> Dict[str, Any]:
        """Perform clustering analysis"""

        log_analysis_start(
            self.dataset_id, "clustering", len(self.df), len(self.df.columns)
        )

        X = self.df.select_dtypes(include=[np.number]).values
        X_scaled = self.scaler.fit_transform(X)

        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        outlier_labels = iso_forest.fit_predict(X_scaled)

        X_clean = X_scaled[outlier_labels != -1]

        if n_clusters is None:
            n_clusters = self._find_optimal_clusters(X_clean, max_clusters)

        results = {}

        try:
            kmeans_result = self._fit_kmeans(X_clean, n_clusters)
            results["kmeans"] = kmeans_result
        except Exception:
            pass

        try:
            dbscan_result = self._fit_dbscan(X_clean)
            results["dbscan"] = dbscan_result
        except Exception:
            pass

        best_result = None
        for algo, result in results.items():
            if best_result is None or result["silhouette"] > best_result["silhouette"]:
                best_result = result
                self.best_algorithm = algo

        if not best_result:
            raise ModelTrainingError(
                message="All clustering algorithms failed",
                error_code="MOD_001",
                severity="MEDIUM",
            )

        cluster_insights = self._generate_cluster_insights(
            best_result["labels"], X_clean
        )

        log_analysis_complete(
            self.dataset_id,
            "clustering",
            0,
            str(self.best_algorithm),
            {"silhouette": float(best_result["silhouette"])},
        )

        return {
            "status": "SUCCESS",
            "algorithm": self.best_algorithm,
            "n_clusters": best_result["n_clusters"],
            "cluster_labels": best_result["labels"].tolist(),
            "clusters": cluster_insights,
            "metrics": {
                "silhouette": best_result["silhouette"],
                "davies_bouldin": best_result["davies_bouldin"],
                "calinski_harabasz": best_result["calinski_harabasz"],
            },
        }

    def _find_optimal_clusters(self, X: np.ndarray, max_clusters: int) -> int:
        """Find optimal number of clusters"""

        silhouette_scores = []

        for n in range(2, min(max_clusters + 1, len(X))):
            try:
                kmeans = KMeans(n_clusters=n, n_init=10, random_state=42)
                labels = kmeans.fit_predict(X)

                sil = silhouette_score(X, labels)
                silhouette_scores.append((n, sil))
            except:
                continue

        if silhouette_scores:
            best = max(silhouette_scores, key=lambda x: x[1])
            return best[0]

        return 2

    def _fit_kmeans(self, X: np.ndarray, n_clusters: int) -> Dict[str, Any]:
        """Fit KMeans model"""

        kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
        labels = kmeans.fit_predict(X)

        sil = silhouette_score(X, labels)
        db = davies_bouldin_score(X, labels)
        ch = calinski_harabasz_score(X, labels)

        return {
            "labels": labels,
            "n_clusters": n_clusters,
            "silhouette": sil,
            "davies_bouldin": db,
            "calinski_harabasz": ch,
            "model": kmeans,
            "type": "kmeans",
        }

    def _fit_dbscan(self, X: np.ndarray) -> Dict[str, Any]:
        """Fit DBSCAN model"""

        dbscan = DBSCAN(eps=0.5, min_samples=5)
        labels = dbscan.fit_predict(X)

        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)

        if n_clusters < 2:
            raise ValueError("DBSCAN found < 2 clusters")

        mask = labels != -1
        X_valid = X[mask]
        labels_valid = labels[mask]

        sil = silhouette_score(X_valid, labels_valid)
        db = davies_bouldin_score(X_valid, labels_valid)
        ch = calinski_harabasz_score(X_valid, labels_valid)

        return {
            "labels": labels,
            "n_clusters": n_clusters,
            "silhouette": sil,
            "davies_bouldin": db,
            "calinski_harabasz": ch,
            "model": dbscan,
            "type": "dbscan",
        }

    def _generate_cluster_insights(
        self, labels: np.ndarray, X: np.ndarray
    ) -> List[Dict[str, Any]]:
        """Generate insights for each cluster"""

        clusters = []

        for cluster_id in sorted(set(labels)):
            mask = labels == cluster_id
            cluster_data = X[mask]

            size = cluster_data.shape[0]
            centroid = cluster_data.mean(axis=0)
            spread = cluster_data.std(axis=0).mean()

            cluster_name = self._name_cluster(cluster_id, centroid)

            clusters.append(
                {
                    "cluster_id": int(cluster_id),
                    "cluster_name": cluster_name,
                    "member_count": int(size),
                    "percentage_of_total": float(size / len(labels) * 100),
                    "centroid": centroid.tolist(),
                    "spread": float(spread),
                    "characteristics": f"Cluster with avg spread {spread:.2f}",
                }
            )

        return clusters

    def _name_cluster(self, cluster_id: int, centroid: np.ndarray) -> str:
        """Auto-generate cluster name"""

        names = ["Premium", "Standard", "Economy", "Emerging", "Niche"]

        if cluster_id < len(names):
            return names[cluster_id]
        else:
            return f"Segment {cluster_id + 1}"
