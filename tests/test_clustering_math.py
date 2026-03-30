import json
import unittest

import numpy as np
import pandas as pd

from app.services.clustering.analysis import (
    _build_clustering_mode_context,
    _build_default_feature_selection_analysis,
    _build_notes,
    _build_sample_weights,
    _compare_clustering_methods,
    _estimate_kmeans_initialization_stability,
    _estimate_resampled_stability,
    _evaluate_cluster_counts,
    _evaluate_feature_subset,
    _run_clustering,
    _select_recommended_method_row,
)
from app.services.clustering.charts import _diagnostic_annotations
from app.services.clustering.core import (
    _build_clustering_quality_assessment,
    _build_cluster_count_guidance,
    _select_render_configuration,
)
from app.services.clustering.data import (
    _aggregate_territory_frame,
    _resolve_selected_features,
    _shrink_rate,
)


FIRE_COUNT = "Число пожаров"
AVG_FIRE_AREA = "Средняя площадь пожара"
NIGHT_SHARE = "Доля ночных пожаров"
AVG_RESPONSE_MINUTES = "Среднее время прибытия, мин"
SEVERE_SHARE = "Доля тяжелых последствий"
NO_WATER_SHARE = "Доля без подтвержденного водоснабжения"
LONG_ARRIVAL_SHARE = "Доля долгих прибытий"
HEATING_SHARE = "Доля пожаров в отопительный сезон"
RESPONSE_COVERAGE = "Покрытие данных по времени прибытия"
TERRITORY_LABEL = "Территория"

LABEL_WORKING_CONFIGURATION = "\u0420\u0430\u0431\u043e\u0447\u0430\u044f \u043a\u043e\u043d\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u044f"
LABEL_RECOMMENDED_CONFIGURATION = "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u0430\u044f \u043a\u043e\u043d\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u044f"
LABEL_K_MODE = "\u0420\u0435\u0436\u0438\u043c \u0432\u044b\u0431\u043e\u0440\u0430 k"
LABEL_METHOD_RECOMMENDATION = "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u044f \u043f\u043e \u043c\u0435\u0442\u043e\u0434\u0443"
LABEL_SEGMENTATION_STRENGTH = "\u0421\u0438\u043b\u0430 \u0441\u0435\u0433\u043c\u0435\u043d\u0442\u0430\u0446\u0438\u0438"
LABEL_WEIGHTING = "\u0412\u0435\u0441\u044b \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439"
LABEL_CLUSTER_COUNT = "\u0427\u0438\u0441\u043b\u043e \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u043e\u0432"
LABEL_LOW_SUPPORT = "\u041d\u0438\u0437\u043a\u0430\u044f \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430"
VALUE_UNIFORM_WEIGHTING = "\u0420\u0430\u0432\u043d\u044b\u0439 \u0432\u0435\u0441 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439"
VALUE_MANUAL_K = "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u0441\u043a\u0438\u0439 k"
VALUE_AUTO_K = "\u0410\u0432\u0442\u043e\u0432\u044b\u0431\u043e\u0440"
SELECTION_RECOMMENDED_CURRENT_K = "\u041b\u0443\u0447\u0448\u0435 \u043d\u0430 \u0442\u0435\u043a\u0443\u0449\u0435\u043c k"
SELECTION_WORKING_AND_RECOMMENDED = "\u0420\u0430\u0431\u043e\u0447\u0438\u0439 \u0438 \u043b\u0443\u0447\u0448\u0438\u0439 \u043d\u0430 \u0442\u0435\u043a\u0443\u0449\u0435\u043c k"


def _methodology_value(quality: dict, label: str) -> str:
    for item in quality["methodology_items"]:
        if item["label"] == label:
            return item["value"]
    raise KeyError(label)


def _methodology_meta(quality: dict, label: str) -> str:
    for item in quality["methodology_items"]:
        if item["label"] == label:
            return item["meta"]
    raise KeyError(label)


def _build_synthetic_frames(
    seed: int = 0,
    cluster_count: int = 2,
    rows_per_cluster: int = 24,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    feature_rows = []
    incident_counts = []
    for cluster_id in range(cluster_count):
        for _ in range(rows_per_cluster):
            incidents = int(rng.integers(2 + (cluster_id * 3), 6 + (cluster_id * 4)))
            incident_counts.append(incidents)
            feature_rows.append(
                {
                    FIRE_COUNT: float(incidents),
                    AVG_FIRE_AREA: float(rng.normal(12.0 + (cluster_id * 18.0), 2.0)),
                    NIGHT_SHARE: float(np.clip(rng.normal(0.10 + (cluster_id * 0.35), 0.04), 0.0, 1.0)),
                    AVG_RESPONSE_MINUTES: float(rng.normal(10.0 + (cluster_id * 7.0), 1.5)),
                    NO_WATER_SHARE: float(np.clip(rng.normal(0.10 + (cluster_id * 0.45), 0.05), 0.0, 1.0)),
                    LONG_ARRIVAL_SHARE: float(rng.uniform(0.0, 1.0)),
                    HEATING_SHARE: float(np.clip(rng.normal(0.25 + (cluster_id * 0.35), 0.05), 0.0, 1.0)),
                    SEVERE_SHARE: float(rng.uniform(0.0, 1.0)),
                    RESPONSE_COVERAGE: 0.95 if cluster_id == 0 else 0.05,
                }
            )
    feature_frame = pd.DataFrame(feature_rows)
    entity_frame = pd.DataFrame({FIRE_COUNT: incident_counts})
    return feature_frame, entity_frame


def _build_low_support_cluster_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    territory_specs = [
        ("Low 0", 10, 1, 0, 1, 8.0, 10.0, 3.0),
        ("Low 1", 11, 1, 0, 1, 8.5, 10.5, 3.1),
        ("Low 2", 12, 0, 1, 0, 9.0, 9.8, 2.8),
        ("Low 3", 9, 1, 0, 0, 8.8, 10.1, 3.2),
        ("High 0", 10, 7, 3, 8, 18.0, 18.5, 8.0),
        ("High 1", 11, 8, 4, 10, 18.5, 19.0, 8.5),
        ("High 2", 12, 9, 5, 11, 19.0, 19.5, 9.0),
        ("High 3", 9, 6, 2, 8, 17.5, 18.2, 7.8),
        ("Outlier High", 1, 1, 1, 1, 60.0, 40.0, 20.0),
        ("Outlier Low", 1, 0, 0, 0, 3.0, 6.0, 1.0),
        ("Outlier Mixed", 1, 1, 0, 0, 25.0, 22.0, 5.0),
    ]
    records = []
    raw_rows: dict[str, tuple[float, float, float]] = {}
    for label, incidents, night_count, severe_count, no_water_count, area, response_minutes, distance in territory_specs:
        raw_rows[label] = (
            night_count / incidents,
            severe_count / incidents,
            no_water_count / incidents,
        )
        for incident_index in range(incidents):
            records.append(
                {
                    "territory_label": label,
                    "district": "Synthetic",
                    "settlement_type": "village",
                    "fire_area": area + (incident_index * 0.1),
                    "response_minutes": response_minutes + (incident_index * 0.05),
                    "long_arrival": incident_index < max(1, incidents // 3),
                    "severe_consequence": incident_index < severe_count,
                    "night_incident": incident_index < night_count,
                    "heating_season": incident_index < max(1, incidents // 2),
                    "has_water_supply": False if incident_index < no_water_count else True,
                    "fire_station_distance": distance,
                }
            )

    aggregated_frame = _aggregate_territory_frame(records)
    entity_frame = aggregated_frame[[FIRE_COUNT]].copy()
    feature_columns = [NIGHT_SHARE, SEVERE_SHARE, NO_WATER_SHARE]
    smoothed_frame = aggregated_frame[feature_columns].copy()
    raw_frame = smoothed_frame.copy()
    territory_order = [str(value) for value in aggregated_frame[TERRITORY_LABEL]]
    for index, territory_name in enumerate(territory_order):
        raw_frame.iloc[index] = raw_rows[territory_name]
    return smoothed_frame, raw_frame, entity_frame


def _build_noise_feature_dataset(seed: int = 123) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    rows = []
    incident_counts = []
    for cluster_id in range(3):
        for _ in range(18):
            rows.append(
                {
                    AVG_FIRE_AREA: 10.0 + (cluster_id * 12.0) + rng.normal(0.0, 1.0),
                    AVG_RESPONSE_MINUTES: 8.0 + (cluster_id * 7.0) + rng.normal(0.0, 0.8),
                    NO_WATER_SHARE: 0.15 + (cluster_id * 0.30) + rng.normal(0.0, 0.03),
                    SEVERE_SHARE: float(rng.uniform(0.0, 1.0)),
                }
            )
            incident_counts.append(4 + cluster_id)
    return pd.DataFrame(rows), pd.DataFrame({FIRE_COUNT: incident_counts})


def _build_method_preference_dataset() -> tuple[pd.DataFrame, pd.DataFrame, int]:
    rng = np.random.default_rng(71)
    cluster_count = int(rng.integers(2, 5))
    rows = []
    incident_counts = []
    for cluster_id in range(cluster_count):
        size = int(rng.integers(12, 36))
        center = rng.normal(0.0, 4.0, size=2)
        variance_x = float(rng.uniform(0.1, 4.0))
        variance_y = float(rng.uniform(0.1, 4.0))
        correlation = float(rng.uniform(-0.95, 0.95))
        covariance = np.array(
            [
                [variance_x, correlation * np.sqrt(variance_x * variance_y)],
                [correlation * np.sqrt(variance_x * variance_y), variance_y],
            ]
        )
        points = rng.multivariate_normal(center, covariance, size=size)
        water_profile = (cluster_id / max(cluster_count - 1, 1)) + rng.normal(0.0, 0.04, size=size)
        for point, water_value in zip(points, water_profile, strict=True):
            rows.append(
                {
                    AVG_FIRE_AREA: float(point[0]),
                    AVG_RESPONSE_MINUTES: float(point[1]),
                    NO_WATER_SHARE: float(water_value),
                }
            )
            incident_counts.append(3)
    return pd.DataFrame(rows), pd.DataFrame({FIRE_COUNT: incident_counts}), cluster_count


class AggregationSmoothingTests(unittest.TestCase):
    def test_empirical_bayes_rate_shrinks_low_support_stronger_than_large_support(self) -> None:
        prior_rate = 0.3
        low_support_rate = _shrink_rate(1.0, 1.0, prior_rate, 3.0)
        higher_support_rate = _shrink_rate(4.0, 4.0, prior_rate, 3.0)

        self.assertLess(abs(low_support_rate - prior_rate), abs(1.0 - prior_rate))
        self.assertLess(abs(low_support_rate - prior_rate), abs(higher_support_rate - prior_rate))

    def test_low_support_territories_are_shrunk_towards_global_profile(self) -> None:
        records = [
            {
                "territory_label": "Low Support",
                "district": "North",
                "settlement_type": "village",
                "fire_area": 80.0,
                "response_minutes": 40.0,
                "long_arrival": True,
                "severe_consequence": True,
                "night_incident": True,
                "heating_season": True,
                "has_water_supply": False,
                "fire_station_distance": 20.0,
            },
        ]
        for _ in range(5):
            records.append(
                {
                    "territory_label": "Stable Day",
                    "district": "North",
                    "settlement_type": "village",
                    "fire_area": 10.0,
                    "response_minutes": 11.0,
                    "long_arrival": False,
                    "severe_consequence": False,
                    "night_incident": False,
                    "heating_season": False,
                    "has_water_supply": True,
                    "fire_station_distance": 3.0,
                }
            )
        for index in range(4):
            records.append(
                {
                    "territory_label": "Mixed",
                    "district": "South",
                    "settlement_type": "settlement",
                    "fire_area": 18.0 + index,
                    "response_minutes": 18.0 + index,
                    "long_arrival": index >= 2,
                    "severe_consequence": index == 0,
                    "night_incident": index % 2 == 0,
                    "heating_season": index >= 1,
                    "has_water_supply": index != 0,
                    "fire_station_distance": 7.0,
                }
            )

        frame = _aggregate_territory_frame(records)
        low_support = frame.loc[frame[TERRITORY_LABEL] == "Low Support"].iloc[0]
        stable = frame.loc[frame[TERRITORY_LABEL] == "Stable Day"].iloc[0]
        global_night_rate = (1 + 0 + 2) / 10

        self.assertLess(low_support[NIGHT_SHARE], 1.0)
        self.assertGreater(low_support[NIGHT_SHARE], stable[NIGHT_SHARE])
        self.assertLess(abs(low_support[NIGHT_SHARE] - global_night_rate), abs(1.0 - global_night_rate))
        self.assertLess(low_support[AVG_FIRE_AREA], 80.0)
        self.assertEqual(low_support["__response_count"], 1)
        self.assertEqual(stable["__response_count"], 5)

    def test_low_support_extremes_have_less_cluster_influence_after_smoothing(self) -> None:
        smoothed_frame, raw_frame, entity_frame = _build_low_support_cluster_frames()

        smoothed_run = _run_clustering(smoothed_frame, entity_frame, 4)
        raw_run = _run_clustering(raw_frame, entity_frame, 4)

        self.assertLess(smoothed_frame.iloc[0][NIGHT_SHARE], raw_frame.iloc[0][NIGHT_SHARE])
        self.assertGreater(int(smoothed_run["smallest_cluster_size"]), int(raw_run["smallest_cluster_size"]))
        self.assertGreater(float(smoothed_run["cluster_balance_ratio"]), float(raw_run["cluster_balance_ratio"]))


class WeightingStrategyTests(unittest.TestCase):
    def test_profile_mode_uses_uniform_weights_while_load_mode_uses_incident_weights(self) -> None:
        _, entity_frame = _build_synthetic_frames(seed=5, cluster_count=3, rows_per_cluster=10)
        profile_context = _build_clustering_mode_context([AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE], None)
        load_context = _build_clustering_mode_context([FIRE_COUNT, AVG_FIRE_AREA, NO_WATER_SHARE], None)

        profile_weights = _build_sample_weights(entity_frame, weighting_strategy=profile_context["weighting_strategy"])
        load_weights = _build_sample_weights(entity_frame, weighting_strategy=load_context["weighting_strategy"])

        self.assertTrue(np.allclose(profile_weights, np.ones(len(profile_weights))))
        self.assertGreater(float(np.std(load_weights)), 0.0)
        self.assertNotEqual(profile_context["weighting_strategy"], load_context["weighting_strategy"])

    def test_profile_mode_notes_and_quality_assessment_explain_uniform_weights(self) -> None:
        mode_context = _build_clustering_mode_context([AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE], None)
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.41,
                "stability_ari": 0.63,
                "initialization_ari": 0.66,
                "smallest_cluster_size": 8,
                "largest_cluster_size": 19,
                "has_microclusters": False,
                "explained_variance": 0.57,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": True,
                    "silhouette": 0.332,
                    "davies_bouldin": 1.041,
                    "calinski_harabasz": 84.2,
                    "cluster_balance_ratio": 0.41,
                }
            ],
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.33},
            feature_selection_report=mode_context,
        )
        notes = _build_notes(
            cluster_profiles=[],
            silhouette=0.332,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4, "elbow_k": 4},
            total_incidents=120,
            total_entities=40,
            sampled_entities=40,
            support_summary={"low_support_share": 0.33},
            stability_ari=0.63,
            feature_selection_report=mode_context,
        )

        text = " ".join(notes + quality["dissertation_points"]).lower()

        self.assertEqual(methodology_map["Веса территорий"], "Равный вес территорий")
        self.assertIn("равным весом территорий", text)
        self.assertNotIn("умеренные log-веса", text)

    def test_load_aware_method_comparison_adds_unweighted_kmeans_control(self) -> None:
        feature_frame, entity_frame = _build_synthetic_frames(seed=17, cluster_count=3, rows_per_cluster=10)
        subset_frame = feature_frame[[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE]].copy()

        method_comparison = _compare_clustering_methods(subset_frame, entity_frame, 3)
        row_map = {row["method_key"]: row for row in method_comparison}

        self.assertIn("kmeans_incident_log", row_map)
        self.assertIn("kmeans_uniform", row_map)
        self.assertTrue(row_map["kmeans_incident_log"]["is_selected"])
        self.assertFalse(row_map["kmeans_uniform"]["is_selected"])
        self.assertEqual(row_map["kmeans_uniform"]["method_label"], "KMeans (равный вес территорий)")
        self.assertEqual(row_map["kmeans_uniform"]["algorithm_key"], "kmeans")


class FeatureSelectionTests(unittest.TestCase):
    def test_default_feature_search_drops_noise_feature(self) -> None:
        feature_frame, entity_frame = _build_synthetic_frames(seed=7)

        report = _build_default_feature_selection_analysis(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            available_features=[
                FIRE_COUNT,
                AVG_FIRE_AREA,
                NIGHT_SHARE,
                AVG_RESPONSE_MINUTES,
                NO_WATER_SHARE,
                LONG_ARRIVAL_SHARE,
                HEATING_SHARE,
            ],
            cluster_count=2,
        )
        selected = report["selected_features"]

        self.assertIn(AVG_FIRE_AREA, selected)
        self.assertNotIn(LONG_ARRIVAL_SHARE, selected)
        self.assertIn(report["volume_role_code"], {"territory_profile", "load_aware", "load_dominant"})

    def test_resolve_selected_features_keeps_auto_defaults_interpretable(self) -> None:
        feature_frame, entity_frame = _build_synthetic_frames(seed=11)

        selected, note = _resolve_selected_features(
            available_features=list(feature_frame.columns),
            requested_features=[],
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            cluster_count=2,
        )

        self.assertGreaterEqual(len(selected), 4)
        self.assertNotIn(SEVERE_SHARE, selected)
        self.assertNotIn(RESPONSE_COVERAGE, selected)
        self.assertIn("автоматически", note)

    def test_removing_noisy_feature_improves_quality_on_controlled_dataset(self) -> None:
        feature_frame, entity_frame = _build_noise_feature_dataset()
        base_features = [AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE]
        noisy_features = [*base_features, SEVERE_SHARE]

        base_result = _evaluate_feature_subset(feature_frame, entity_frame, base_features, 3)
        noisy_result = _evaluate_feature_subset(feature_frame, entity_frame, noisy_features, 3)

        self.assertGreater(base_result["score"], noisy_result["score"])
        self.assertGreater(base_result["silhouette"], noisy_result["silhouette"])
        self.assertLess(base_result["davies_bouldin"], noisy_result["davies_bouldin"])


class DiagnosticsAndStabilityTests(unittest.TestCase):
    def test_cluster_diagnostics_do_not_leave_ui_range(self) -> None:
        cluster_frame, entity_frame = _build_synthetic_frames(seed=19, cluster_count=7, rows_per_cluster=10)

        diagnostics = _evaluate_cluster_counts(cluster_frame, entity_frame)

        ks = [row["cluster_count"] for row in diagnostics["rows"]]
        self.assertTrue(ks)
        self.assertEqual(min(ks), 2)
        self.assertLessEqual(max(ks), 6)
        self.assertIn(diagnostics["best_silhouette_k"], {2, 3, 4, 5, 6})
        self.assertIn(diagnostics["best_quality_k"], {2, 3, 4, 5, 6})

    def test_resampled_stability_is_stricter_than_init_only_on_ambiguous_cloud(self) -> None:
        points = np.array(
            [[value, 0.0] for value in (-6.0, -5.0, -4.0, -1.0, 0.0, 1.0, 4.0, 5.0, 6.0)],
            dtype=float,
        )
        weights = np.ones(len(points), dtype=float)

        init_stability = _estimate_kmeans_initialization_stability(points, cluster_count=2, sample_weights=weights)
        resampled_stability = _estimate_resampled_stability(points, cluster_count=2, sample_weights=weights)

        self.assertIsNotNone(init_stability)
        self.assertIsNotNone(resampled_stability)
        self.assertLess(float(resampled_stability), float(init_stability))

    def test_resampled_stability_distinguishes_stable_and_ambiguous_structures(self) -> None:
        stable_rng = np.random.default_rng(42)
        stable_points = np.vstack(
            [
                stable_rng.normal(loc=(-3.0, -3.0), scale=0.35, size=(60, 2)),
                stable_rng.normal(loc=(3.0, 3.0), scale=0.35, size=(60, 2)),
            ]
        )
        ambiguous_points = np.random.default_rng(43).normal(loc=(0.0, 0.0), scale=1.2, size=(120, 2))

        stable_score = _estimate_resampled_stability(stable_points, cluster_count=2, sample_weights=np.ones(len(stable_points)))
        ambiguous_score = _estimate_resampled_stability(
            ambiguous_points,
            cluster_count=4,
            sample_weights=np.ones(len(ambiguous_points)),
        )

        self.assertIsNotNone(stable_score)
        self.assertIsNotNone(ambiguous_score)
        self.assertGreater(float(stable_score), 0.9)
        self.assertLess(float(ambiguous_score), 0.75)
        self.assertGreater(float(stable_score), float(ambiguous_score) + 0.2)

    def test_diagnostic_annotations_show_current_and_recommended_k_separately(self) -> None:
        annotations = _diagnostic_annotations(
            rows=[
                {"cluster_count": 2, "silhouette": 0.31},
                {"cluster_count": 3, "silhouette": 0.34},
                {"cluster_count": 4, "silhouette": 0.36},
            ],
            current_cluster_count=4,
            recommended_cluster_count=2,
            best_silhouette_k=4,
            elbow_k=3,
        )

        labels = {item["text"] for item in annotations}
        self.assertIn("Рабочий k", labels)
        self.assertIn("Рекомендуемый k", labels)
        self.assertIn("Лучший silhouette", labels)


class QualityAssessmentTests(unittest.TestCase):
    def test_cluster_count_guidance_warns_when_recommended_k_differs_from_current(self) -> None:
        guidance = _build_cluster_count_guidance(
            requested_cluster_count=4,
            current_cluster_count=4,
            diagnostics={"best_quality_k": 2, "best_silhouette_k": 4},
        )

        self.assertTrue(guidance["has_recommendation_gap"])
        self.assertIn("не переключал число кластеров автоматически", guidance["suggested_note"])
        self.assertIn("диагностика рекомендует k=2", guidance["current_note"].lower())
        self.assertIn("рабочий k=4", guidance["methodology_meta"])
        self.assertIn("рекомендуемое k=2", guidance["methodology_meta"])

    def test_recommended_method_prefers_better_quality_without_size_degradation(self) -> None:
        recommended = _select_recommended_method_row(
            [
                {
                    "method_key": "kmeans",
                    "score": 0.40,
                    "quality_score": 0.40,
                    "silhouette": 0.31,
                    "davies_bouldin": 1.10,
                    "cluster_balance_ratio": 0.20,
                    "smallest_cluster_size": 5,
                    "shape_penalty": 0.02,
                    "has_microclusters": False,
                },
                {
                    "method_key": "agglomerative",
                    "score": 0.43,
                    "quality_score": 0.43,
                    "silhouette": 0.34,
                    "davies_bouldin": 1.02,
                    "cluster_balance_ratio": 0.18,
                    "smallest_cluster_size": 4,
                    "shape_penalty": 0.02,
                    "has_microclusters": False,
                },
            ]
        )

        self.assertIsNotNone(recommended)
        self.assertEqual(recommended["method_key"], "agglomerative")

    def test_recommended_method_can_switch_to_unweighted_kmeans_when_weighted_version_loses(self) -> None:
        method_comparison = [
            {
                "method_key": "kmeans_incident_log",
                "algorithm_key": "kmeans",
                "method_label": "KMeans (умеренный вес по числу пожаров)",
                "is_selected": True,
                "is_recommended": False,
                "quality_score": 0.41,
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.24,
                "smallest_cluster_size": 6,
                "largest_cluster_size": 25,
                "shape_penalty": 0.03,
                "has_microclusters": False,
            },
            {
                "method_key": "kmeans_uniform",
                "algorithm_key": "kmeans",
                "method_label": "KMeans (равный вес территорий)",
                "is_selected": False,
                "is_recommended": True,
                "quality_score": 0.45,
                "silhouette": 0.361,
                "davies_bouldin": 0.982,
                "calinski_harabasz": 92.4,
                "cluster_balance_ratio": 0.22,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 23,
                "shape_penalty": 0.02,
                "has_microclusters": False,
            },
            {
                "method_key": "agglomerative",
                "algorithm_key": "agglomerative",
                "method_label": "Агломеративная кластеризация (Ward)",
                "is_selected": False,
                "is_recommended": False,
                "quality_score": 0.43,
                "silhouette": 0.347,
                "davies_bouldin": 0.994,
                "calinski_harabasz": 89.8,
                "cluster_balance_ratio": 0.20,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 24,
                "shape_penalty": 0.02,
                "has_microclusters": False,
            },
        ]

        recommended = _select_recommended_method_row(method_comparison)
        self.assertIsNotNone(recommended)
        self.assertEqual(recommended["method_key"], "kmeans_uniform")

        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.24,
                "smallest_cluster_size": 6,
                "largest_cluster_size": 25,
                "has_microclusters": False,
                "stability_ari": 0.66,
                "initialization_ari": 0.74,
                "explained_variance": 0.58,
            },
            method_comparison=method_comparison,
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.2},
        )

        methodology_map = {item["label"]: item["value"] for item in quality["methodology_items"]}
        dissertation_text = " ".join(quality["dissertation_points"]).lower()
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}

        self.assertEqual(methodology_map["Рекомендация по методу"], "KMeans (равный вес территорий)")
        self.assertIn("стратегии весов", dissertation_text)
        self.assertEqual(comparison_labels["KMeans (равный вес территорий)"], "Рекомендовано")

    def test_quality_assessment_mentions_resampling_and_low_support(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.312,
                "davies_bouldin": 1.192,
                "calinski_harabasz": 206.4,
                "cluster_balance_ratio": 0.33,
                "stability_ari": 0.58,
                "initialization_ari": 0.91,
                "smallest_cluster_size": 7,
                "largest_cluster_size": 21,
                "has_microclusters": False,
                "explained_variance": 0.61,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": True,
                    "silhouette": 0.312,
                    "davies_bouldin": 1.192,
                    "calinski_harabasz": 206.4,
                    "cluster_balance_ratio": 0.33,
                }
            ],
            cluster_count=4,
            selected_features=[FIRE_COUNT, AVG_FIRE_AREA, NIGHT_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.633},
        )

        methodology_labels = [item["label"] for item in quality["methodology_items"]]
        dissertation_text = " ".join(quality["dissertation_points"]).lower()
        metric_meta = " ".join(item["meta"] for item in quality["metric_cards"]).lower()

        self.assertIn("Низкая поддержка", methodology_labels)
        self.assertIn("подвыбор", quality["subtitle"].lower())
        self.assertIn("подвыбор", metric_meta)
        self.assertIn("не более 2 пожаров", dissertation_text)
        self.assertIn("empirical bayes", dissertation_text)

    def test_quality_assessment_explains_when_working_k_differs_from_recommended(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.421,
                "davies_bouldin": 0.941,
                "calinski_harabasz": 88.2,
                "cluster_balance_ratio": 0.21,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 24,
                "has_microclusters": False,
                "stability_ari": 0.76,
                "initialization_ari": 0.97,
                "explained_variance": 0.63,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": True,
                    "silhouette": 0.421,
                    "davies_bouldin": 0.941,
                    "calinski_harabasz": 88.2,
                    "cluster_balance_ratio": 0.21,
                },
            ],
            cluster_count=4,
            requested_cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES],
            diagnostics={"best_quality_k": 2, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.2},
        )

        methodology_map = {item["label"]: item["meta"] for item in quality["methodology_items"]}
        dissertation_text = " ".join(quality["dissertation_points"]).lower()

        self.assertIn("рабочий k=4", methodology_map["Число кластеров"])
        self.assertIn("рекомендуемое k=2", methodology_map["Число кластеров"])
        self.assertIn("не переключает число кластеров автоматически", dissertation_text)

    def test_quality_assessment_can_recommend_alternative_method_and_warn_about_microclusters(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.287,
                "davies_bouldin": 1.108,
                "calinski_harabasz": 98.4,
                "cluster_balance_ratio": 0.08,
                "smallest_cluster_size": 2,
                "largest_cluster_size": 25,
                "microcluster_threshold": 4,
                "has_microclusters": True,
                "stability_ari": 0.42,
                "initialization_ari": 0.96,
                "explained_variance": 0.58,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": False,
                    "silhouette": 0.287,
                    "davies_bouldin": 1.108,
                    "calinski_harabasz": 98.4,
                    "cluster_balance_ratio": 0.08,
                },
                {
                    "method_key": "agglomerative",
                    "method_label": "Агломеративная кластеризация (Ward)",
                    "is_selected": False,
                    "is_recommended": True,
                    "silhouette": 0.321,
                    "davies_bouldin": 1.004,
                    "calinski_harabasz": 104.2,
                    "cluster_balance_ratio": 0.24,
                },
            ],
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 5},
            support_summary={"low_support_share": 0.4},
        )

        methodology_map = {item["label"]: item["value"] for item in quality["methodology_items"]}
        dissertation_text = " ".join(quality["dissertation_points"]).lower()
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}

        self.assertEqual(methodology_map["Рекомендация по методу"], "Агломеративная кластеризация (Ward)")
        self.assertEqual(methodology_map["Сила сегментации"], "Слабая")
        self.assertIn("микрокластер", dissertation_text)
        self.assertIn("random_state", dissertation_text)
        self.assertIn("ward", dissertation_text)
        self.assertEqual(comparison_labels["Агломеративная кластеризация (Ward)"], "Рекомендовано")

    def test_actual_method_comparison_can_recommend_alternative_method(self) -> None:
        feature_frame, entity_frame, cluster_count = _build_method_preference_dataset()

        method_comparison = _compare_clustering_methods(feature_frame, entity_frame, cluster_count)
        clustering = _run_clustering(feature_frame, entity_frame, cluster_count)
        quality = _build_clustering_quality_assessment(
            clustering=clustering,
            method_comparison=method_comparison,
            cluster_count=cluster_count,
            selected_features=list(feature_frame.columns),
            diagnostics={"best_quality_k": cluster_count, "best_silhouette_k": cluster_count},
            support_summary={"low_support_share": 0.0},
        )

        recommended_row = next((row for row in method_comparison if row.get("is_recommended")), None)
        methodology_map = {item["label"]: item["value"] for item in quality["methodology_items"]}
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}

        self.assertIsNotNone(recommended_row)
        self.assertEqual(recommended_row["method_key"], "agglomerative")
        self.assertEqual(methodology_map["Рекомендация по методу"], "Агломеративная кластеризация (Ward)")
        self.assertEqual(comparison_labels["Агломеративная кластеризация (Ward)"], "Рекомендовано")

    def test_quality_assessment_softens_strength_when_current_method_is_not_recommended(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.421,
                "davies_bouldin": 0.941,
                "calinski_harabasz": 88.2,
                "cluster_balance_ratio": 0.21,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 24,
                "has_microclusters": False,
                "stability_ari": 0.76,
                "initialization_ari": 0.97,
                "explained_variance": 0.63,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": False,
                    "silhouette": 0.421,
                    "davies_bouldin": 0.941,
                    "calinski_harabasz": 88.2,
                    "cluster_balance_ratio": 0.21,
                },
                {
                    "method_key": "agglomerative",
                    "method_label": "Агломеративная кластеризация (Ward)",
                    "is_selected": False,
                    "is_recommended": True,
                    "silhouette": 0.438,
                    "davies_bouldin": 0.932,
                    "calinski_harabasz": 91.5,
                    "cluster_balance_ratio": 0.19,
                },
            ],
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 5},
            support_summary={"low_support_share": 0.2},
        )

        methodology_map = {item["label"]: item["value"] for item in quality["methodology_items"]}
        dissertation_text = " ".join(quality["dissertation_points"]).lower()

        self.assertEqual(methodology_map["Сила сегментации"], "Умеренная")
        self.assertIn("альтернативный метод", dissertation_text)


_BaseWeightingStrategyTests = WeightingStrategyTests


class WeightingStrategyTests(_BaseWeightingStrategyTests):
    def test_profile_mode_notes_and_quality_assessment_explain_uniform_weights(self) -> None:
        mode_context = _build_clustering_mode_context([AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE], None)
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.41,
                "stability_ari": 0.63,
                "initialization_ari": 0.66,
                "smallest_cluster_size": 8,
                "largest_cluster_size": 19,
                "has_microclusters": False,
                "explained_variance": 0.57,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": True,
                    "silhouette": 0.332,
                    "davies_bouldin": 1.041,
                    "calinski_harabasz": 84.2,
                    "cluster_balance_ratio": 0.41,
                }
            ],
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.33},
            feature_selection_report=mode_context,
        )
        notes = _build_notes(
            cluster_profiles=[],
            silhouette=0.332,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4, "elbow_k": 4},
            total_incidents=120,
            total_entities=40,
            sampled_entities=40,
            support_summary={"low_support_share": 0.33},
            stability_ari=0.63,
            feature_selection_report=mode_context,
        )

        text = " ".join(notes + quality["dissertation_points"]).lower()

        self.assertEqual(_methodology_value(quality, LABEL_WEIGHTING), VALUE_UNIFORM_WEIGHTING)
        self.assertIn(mode_context["weighting_note"].lower(), text)
        self.assertNotIn("\u0443\u043c\u0435\u0440\u0435\u043d\u043d\u044b\u0435 log-\u0432\u0435\u0441\u0430", text)

    def test_load_aware_method_comparison_adds_unweighted_kmeans_control(self) -> None:
        feature_frame, entity_frame = _build_synthetic_frames(seed=17, cluster_count=3, rows_per_cluster=10)
        subset_frame = feature_frame[[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE]].copy()

        method_comparison = _compare_clustering_methods(subset_frame, entity_frame, 3)
        row_map = {row["method_key"]: row for row in method_comparison}

        self.assertIn("kmeans_incident_log", row_map)
        self.assertIn("kmeans_uniform", row_map)
        self.assertTrue(row_map["kmeans_incident_log"]["is_selected"])
        self.assertFalse(row_map["kmeans_uniform"]["is_selected"])
        self.assertEqual(row_map["kmeans_uniform"]["method_label"], f"KMeans ({VALUE_UNIFORM_WEIGHTING.lower()})")
        self.assertEqual(row_map["kmeans_uniform"]["algorithm_key"], "kmeans")


class RenderConfigurationTests(unittest.TestCase):
    def test_auto_render_configuration_uses_best_configuration(self) -> None:
        diagnostics = {
            "best_configuration": {
                "cluster_count": 2,
                "method_key": "agglomerative",
                "algorithm_key": "agglomerative",
                "method_label": "\u0410\u0433\u043b\u043e\u043c\u0435\u0440\u0430\u0442\u0438\u0432\u043d\u0430\u044f \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f (Ward)",
                "weighting_strategy": "not_applicable",
            },
            "method_rows_by_cluster_count": {
                4: [
                    {
                        "method_key": "kmeans_uniform",
                        "algorithm_key": "kmeans",
                        "method_label": f"KMeans ({VALUE_UNIFORM_WEIGHTING.lower()})",
                        "weighting_strategy": "uniform",
                        "is_selected": False,
                        "is_recommended": True,
                    }
                ]
            },
        }

        configuration = _select_render_configuration(
            requested_cluster_count=4,
            cluster_count_is_explicit=False,
            diagnostics=diagnostics,
            fallback_weighting_strategy="incident_log",
        )

        self.assertEqual(configuration["cluster_count"], 2)
        self.assertEqual(configuration["method_key"], "agglomerative")
        self.assertEqual(configuration["algorithm_key"], "agglomerative")

    def test_manual_render_configuration_keeps_requested_k_but_uses_best_method_at_that_k(self) -> None:
        diagnostics = {
            "best_configuration": {
                "cluster_count": 2,
                "method_key": "agglomerative",
                "algorithm_key": "agglomerative",
                "method_label": "\u0410\u0433\u043b\u043e\u043c\u0435\u0440\u0430\u0442\u0438\u0432\u043d\u0430\u044f \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f (Ward)",
                "weighting_strategy": "not_applicable",
            },
            "method_rows_by_cluster_count": {
                4: [
                    {
                        "method_key": "kmeans_incident_log",
                        "algorithm_key": "kmeans",
                        "method_label": "KMeans (\u0443\u043c\u0435\u0440\u0435\u043d\u043d\u044b\u0439 \u0432\u0435\u0441 \u043f\u043e \u0447\u0438\u0441\u043b\u0443 \u043f\u043e\u0436\u0430\u0440\u043e\u0432)",
                        "weighting_strategy": "incident_log",
                        "is_selected": True,
                        "is_recommended": False,
                    },
                    {
                        "method_key": "kmeans_uniform",
                        "algorithm_key": "kmeans",
                        "method_label": f"KMeans ({VALUE_UNIFORM_WEIGHTING.lower()})",
                        "weighting_strategy": "uniform",
                        "is_selected": False,
                        "is_recommended": True,
                    },
                ]
            },
        }

        configuration = _select_render_configuration(
            requested_cluster_count=4,
            cluster_count_is_explicit=True,
            diagnostics=diagnostics,
            fallback_weighting_strategy="incident_log",
        )

        self.assertEqual(configuration["cluster_count"], 4)
        self.assertEqual(configuration["method_key"], "kmeans_uniform")
        self.assertEqual(configuration["algorithm_key"], "kmeans")


_BaseQualityAssessmentTests = QualityAssessmentTests


class QualityAssessmentTests(_BaseQualityAssessmentTests):
    def test_cluster_count_guidance_warns_when_recommended_k_differs_from_current(self) -> None:
        guidance = _build_cluster_count_guidance(
            requested_cluster_count=4,
            current_cluster_count=4,
            diagnostics={"best_quality_k": 2, "best_silhouette_k": 4},
            cluster_count_is_explicit=True,
        )

        self.assertTrue(guidance["has_recommendation_gap"])
        self.assertEqual(guidance["suggested_label"], "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u044b\u0439 k")
        self.assertIn("k=2", guidance["current_note"])
        self.assertIn("k=4", guidance["current_note"])
        self.assertIn("\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0437\u0430\u0444\u0438\u043a\u0441\u0438\u0440\u043e\u0432\u0430\u043b k=4", guidance["model_note"].lower())
        self.assertIn("\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u0441\u043a\u0438\u0439 k=4", guidance["methodology_meta"])
        self.assertIn("\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u043e\u0435 k=2", guidance["methodology_meta"])

    def test_cluster_count_guidance_auto_mode_reports_synced_recommended_k(self) -> None:
        guidance = _build_cluster_count_guidance(
            requested_cluster_count=4,
            current_cluster_count=2,
            diagnostics={"best_quality_k": 2, "best_silhouette_k": 4},
            cluster_count_is_explicit=False,
        )

        self.assertFalse(guidance["has_recommendation_gap"])
        self.assertEqual(guidance["suggested_label"], "\u0410\u0432\u0442\u043e\u0432\u044b\u0431\u043e\u0440 k")
        self.assertIn(
            "\u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u0442 \u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u044b\u0439 k=2",
            guidance["current_note"].lower(),
        )
        self.assertIn("\u0432\u043c\u0435\u0441\u0442\u043e \u0441\u0442\u0430\u0440\u0442\u043e\u0432\u043e\u0433\u043e k=4", guidance["current_note"].lower())
        self.assertNotIn("\u0438\u0437-\u0437\u0430 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439", guidance["current_note"].lower())
        self.assertIn("\u0440\u0430\u0431\u043e\u0447\u0438\u0439 k=2", guidance["methodology_meta"])
        self.assertIn("\u0441\u0442\u0430\u0440\u0442\u043e\u0432\u044b\u0439 k=4", guidance["methodology_meta"])
        self.assertIn("\u043f\u0438\u043a silhouette \u043d\u0430 k=4", guidance["methodology_meta"])

    def test_recommended_method_can_switch_to_unweighted_kmeans_when_weighted_version_loses(self) -> None:
        method_comparison = [
            {
                "method_key": "kmeans_incident_log",
                "algorithm_key": "kmeans",
                "method_label": "KMeans (\u0443\u043c\u0435\u0440\u0435\u043d\u043d\u044b\u0439 \u0432\u0435\u0441 \u043f\u043e \u0447\u0438\u0441\u043b\u0443 \u043f\u043e\u0436\u0430\u0440\u043e\u0432)",
                "is_selected": True,
                "is_recommended": False,
                "quality_score": 0.41,
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.24,
                "smallest_cluster_size": 6,
                "largest_cluster_size": 25,
                "shape_penalty": 0.03,
                "has_microclusters": False,
            },
            {
                "method_key": "kmeans_uniform",
                "algorithm_key": "kmeans",
                "method_label": f"KMeans ({VALUE_UNIFORM_WEIGHTING.lower()})",
                "is_selected": False,
                "is_recommended": True,
                "quality_score": 0.45,
                "silhouette": 0.361,
                "davies_bouldin": 0.982,
                "calinski_harabasz": 92.4,
                "cluster_balance_ratio": 0.22,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 23,
                "shape_penalty": 0.02,
                "has_microclusters": False,
            },
            {
                "method_key": "agglomerative",
                "algorithm_key": "agglomerative",
                "method_label": "\u0410\u0433\u043b\u043e\u043c\u0435\u0440\u0430\u0442\u0438\u0432\u043d\u0430\u044f \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f (Ward)",
                "is_selected": False,
                "is_recommended": False,
                "quality_score": 0.43,
                "silhouette": 0.347,
                "davies_bouldin": 0.994,
                "calinski_harabasz": 89.8,
                "cluster_balance_ratio": 0.20,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 24,
                "shape_penalty": 0.02,
                "has_microclusters": False,
            },
        ]

        recommended = _select_recommended_method_row(method_comparison)
        self.assertIsNotNone(recommended)
        self.assertEqual(recommended["method_key"], "kmeans_uniform")

        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.332,
                "davies_bouldin": 1.041,
                "calinski_harabasz": 84.2,
                "cluster_balance_ratio": 0.24,
                "smallest_cluster_size": 6,
                "largest_cluster_size": 25,
                "has_microclusters": False,
                "stability_ari": 0.66,
                "initialization_ari": 0.74,
                "explained_variance": 0.58,
            },
            method_comparison=method_comparison,
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.2},
        )

        dissertation_text = " ".join(quality["dissertation_points"]).lower()
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}

        self.assertEqual(_methodology_value(quality, LABEL_METHOD_RECOMMENDATION), method_comparison[1]["method_label"])
        self.assertIn(method_comparison[1]["method_label"], _methodology_value(quality, LABEL_RECOMMENDED_CONFIGURATION))
        self.assertIn("\u0432\u0435\u0441\u043e\u0432", dissertation_text)
        self.assertEqual(comparison_labels[method_comparison[1]["method_label"]], SELECTION_RECOMMENDED_CURRENT_K)

    def test_quality_assessment_explains_when_working_k_differs_from_recommended(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.421,
                "davies_bouldin": 0.941,
                "calinski_harabasz": 88.2,
                "cluster_balance_ratio": 0.21,
                "smallest_cluster_size": 5,
                "largest_cluster_size": 24,
                "has_microclusters": False,
                "stability_ari": 0.76,
                "initialization_ari": 0.97,
                "explained_variance": 0.63,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": True,
                    "silhouette": 0.421,
                    "davies_bouldin": 0.941,
                    "calinski_harabasz": 88.2,
                    "cluster_balance_ratio": 0.21,
                },
            ],
            cluster_count=4,
            requested_cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES],
            diagnostics={"best_quality_k": 2, "best_silhouette_k": 4},
            support_summary={"low_support_share": 0.2},
            cluster_count_is_explicit=True,
        )

        dissertation_text = " ".join(quality["dissertation_points"]).lower()

        self.assertEqual(_methodology_value(quality, LABEL_K_MODE), VALUE_MANUAL_K)
        self.assertIn("\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u0441\u043a\u0438\u0439 k=4", _methodology_meta(quality, LABEL_CLUSTER_COUNT))
        self.assertIn("\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u043e\u0435 k=2", _methodology_meta(quality, LABEL_CLUSTER_COUNT))
        self.assertIn("\u043d\u0435 \u0441\u043e\u0432\u043f\u0430\u0434\u0430\u0435\u0442", dissertation_text)
        self.assertIn("k=2", dissertation_text)

    def test_quality_assessment_can_recommend_alternative_method_and_warn_about_microclusters(self) -> None:
        quality = _build_clustering_quality_assessment(
            clustering={
                "silhouette": 0.287,
                "davies_bouldin": 1.108,
                "calinski_harabasz": 98.4,
                "cluster_balance_ratio": 0.08,
                "smallest_cluster_size": 2,
                "largest_cluster_size": 25,
                "microcluster_threshold": 4,
                "has_microclusters": True,
                "stability_ari": 0.42,
                "initialization_ari": 0.96,
                "explained_variance": 0.58,
            },
            method_comparison=[
                {
                    "method_key": "kmeans",
                    "method_label": "KMeans",
                    "is_selected": True,
                    "is_recommended": False,
                    "silhouette": 0.287,
                    "davies_bouldin": 1.108,
                    "calinski_harabasz": 98.4,
                    "cluster_balance_ratio": 0.08,
                },
                {
                    "method_key": "agglomerative",
                    "method_label": "\u0410\u0433\u043b\u043e\u043c\u0435\u0440\u0430\u0442\u0438\u0432\u043d\u0430\u044f \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f (Ward)",
                    "is_selected": False,
                    "is_recommended": True,
                    "silhouette": 0.321,
                    "davies_bouldin": 1.004,
                    "calinski_harabasz": 104.2,
                    "cluster_balance_ratio": 0.24,
                },
            ],
            cluster_count=4,
            selected_features=[AVG_FIRE_AREA, AVG_RESPONSE_MINUTES],
            diagnostics={"best_quality_k": 4, "best_silhouette_k": 5},
            support_summary={"low_support_share": 0.4},
        )

        dissertation_text = " ".join(quality["dissertation_points"]).lower()
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}
        recommended_method_label = _methodology_value(quality, LABEL_METHOD_RECOMMENDATION)

        self.assertIn("Ward", recommended_method_label)
        self.assertEqual(_methodology_value(quality, LABEL_SEGMENTATION_STRENGTH), "\u0421\u043b\u0430\u0431\u0430\u044f")
        self.assertIn("\u043c\u0438\u043a\u0440\u043e\u043a\u043b\u0430\u0441\u0442\u0435\u0440", dissertation_text)
        self.assertIn("random_state", dissertation_text)
        self.assertIn("ward", dissertation_text)
        self.assertEqual(comparison_labels[recommended_method_label], SELECTION_RECOMMENDED_CURRENT_K)

    def test_actual_method_comparison_can_recommend_alternative_method(self) -> None:
        feature_frame, entity_frame, cluster_count = _build_method_preference_dataset()

        method_comparison = _compare_clustering_methods(feature_frame, entity_frame, cluster_count)
        clustering = _run_clustering(feature_frame, entity_frame, cluster_count)
        quality = _build_clustering_quality_assessment(
            clustering=clustering,
            method_comparison=method_comparison,
            cluster_count=cluster_count,
            selected_features=list(feature_frame.columns),
            diagnostics={"best_quality_k": cluster_count, "best_silhouette_k": cluster_count},
            support_summary={"low_support_share": 0.0},
        )

        recommended_row = next((row for row in method_comparison if row.get("is_recommended")), None)
        comparison_labels = {row["method_label"]: row["selection_label"] for row in quality["comparison_rows"]}

        self.assertIsNotNone(recommended_row)
        self.assertEqual(recommended_row["method_key"], "agglomerative")
        self.assertEqual(_methodology_value(quality, LABEL_METHOD_RECOMMENDATION), recommended_row["method_label"])
        self.assertEqual(comparison_labels[recommended_row["method_label"]], SELECTION_RECOMMENDED_CURRENT_K)


del _BaseWeightingStrategyTests
del _BaseQualityAssessmentTests


class NotesAndPayloadTests(unittest.TestCase):
    def test_notes_and_quality_payload_remain_json_serializable(self) -> None:
        feature_frame, entity_frame = _build_synthetic_frames(seed=19, cluster_count=7, rows_per_cluster=10)
        selected_features = [FIRE_COUNT, AVG_FIRE_AREA, AVG_RESPONSE_MINUTES, NO_WATER_SHARE]
        subset_frame = feature_frame[selected_features].copy()
        diagnostics = _evaluate_cluster_counts(subset_frame, entity_frame)
        clustering = _run_clustering(subset_frame, entity_frame, 4)
        method_comparison = _compare_clustering_methods(subset_frame, entity_frame, 4)
        notes = _build_notes(
            cluster_profiles=[
                {
                    "cluster_label": "Тип 1",
                    "size_display": "18",
                    "share_display": "25%",
                    "segment_title": "умеренный риск",
                }
            ],
            silhouette=clustering["silhouette"],
            selected_features=selected_features,
            diagnostics=diagnostics,
            total_incidents=int(entity_frame[FIRE_COUNT].sum()),
            total_entities=len(feature_frame),
            sampled_entities=len(feature_frame),
            support_summary={"low_support_share": 0.33},
            stability_ari=clustering["stability_ari"],
            feature_selection_report={
                "volume_note": "Кластеризация описывает профиль территории без вывода за пределы UI-диапазона.",
                "ablation_rows": [],
            },
        )
        quality = _build_clustering_quality_assessment(
            clustering=clustering,
            method_comparison=method_comparison,
            cluster_count=4,
            selected_features=selected_features,
            diagnostics=diagnostics,
            support_summary={"low_support_share": 0.33},
        )

        self.assertTrue(notes)
        self.assertTrue(all(isinstance(note, str) and note.strip() for note in notes))
        self.assertNotIn("k=7", " ".join(notes))
        self.assertNotIn("k=8", " ".join(notes))
        self.assertTrue(quality["metric_cards"])
        self.assertTrue(quality["methodology_items"])
        self.assertTrue(quality["comparison_rows"])

        payload = {"notes": notes, "quality_assessment": quality}
        serialized = json.dumps(payload, ensure_ascii=False)
        self.assertIn("quality_assessment", serialized)
        self.assertIn("notes", serialized)


if __name__ == "__main__":
    unittest.main()
