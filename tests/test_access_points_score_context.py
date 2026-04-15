from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.access_points import analysis_output
from app.services.access_points.analysis_factors import (
    FACTOR_WEIGHTS,
    UNCERTAINTY_CODE,
    UNCERTAINTY_PENALTY_MAX,
    _resolve_access_point_weight_context,
)
from app.services.access_points.analysis_output_context import _score_total_and_uncertainty_penalty
from app.services.access_points.analysis_output_types import _AccessPointDisplayContext, _AccessPointRowMetrics


def _build_metrics() -> _AccessPointRowMetrics:
    return _AccessPointRowMetrics(
        incident_count=0,
        years_observed=1,
        incidents_per_year=0.0,
        average_distance=None,
        average_response=None,
        long_arrival_share=0.0,
        no_water_share=0.0,
        water_coverage_share=1.0,
        water_unknown_share=0.0,
        severe_share=0.0,
        night_share=0.0,
        heating_share=0.0,
        rural_share=0.0,
        response_coverage_share=1.0,
        distance_coverage_share=1.0,
        arrival_missing_share=0.0,
        distance_missing_share=0.0,
        completeness_share=1.0,
        distance_norm=0.0,
        response_norm=0.0,
        support_weight=1.0,
        severity_factor=0.0,
        recurrence_factor=0.0,
        uncertainty_factor=0.0,
        access_score=0.0,
        water_score=0.0,
        severity_score=0.0,
        recurrence_score=0.0,
        data_gap_score=0.0,
        latitude=None,
        longitude=None,
    )


def _build_displays() -> _AccessPointDisplayContext:
    return _AccessPointDisplayContext(
        average_distance_display="н/д",
        average_response_display="н/д",
        response_coverage_display="100%",
        long_arrival_share_display="0%",
        no_water_share_display="0%",
        water_unknown_share_display="0%",
        water_coverage_display="100%",
        severe_share_display="0%",
        night_share_display="0%",
        heating_share_display="0%",
        rural_share_display="0%",
        completeness_display="100%",
    )


class AccessPointScoreContextTests(unittest.TestCase):
    def test_no_uncertainty_penalty_uses_only_main_term(self) -> None:
        score_decomposition = [
            {"code": "DISTANCE_TO_STATION", "contribution_points": 20.0},
            {"code": "RESPONSE_TIME", "contribution_points": 10.0},
            {"code": UNCERTAINTY_CODE, "contribution_points": 0.0, "is_penalty": True},
        ]
        pure_score, uncertainty_penalty = _score_total_and_uncertainty_penalty(score_decomposition)
        total_score = pure_score + uncertainty_penalty
        self.assertAlmostEqual(total_score, pure_score, places=6)
        with patch.object(analysis_output, "_build_access_point_score_decomposition", return_value=score_decomposition):
            context = analysis_output._build_access_point_score_context(
                _build_metrics(),
                _build_displays(),
                active_reason_codes=set(FACTOR_WEIGHTS.keys()),
                normalized_factor_weights={},
            )
        self.assertAlmostEqual(context.total_score, 30.0, places=6)
        self.assertAlmostEqual(context.investigation_score, 0.72 * context.total_score, places=6)
        self.assertLessEqual(context.investigation_score, context.total_score + 30.0)

    def test_full_uncertainty_penalty_does_not_double_count_in_main_term(self) -> None:
        score_decomposition = [
            {"code": UNCERTAINTY_CODE, "contribution_points": UNCERTAINTY_PENALTY_MAX, "is_penalty": True},
        ]
        pure_score, uncertainty_penalty = _score_total_and_uncertainty_penalty(score_decomposition)
        self.assertAlmostEqual(pure_score, 0.0, places=6)
        self.assertAlmostEqual(uncertainty_penalty, UNCERTAINTY_PENALTY_MAX, places=6)
        with patch.object(analysis_output, "_build_access_point_score_decomposition", return_value=score_decomposition):
            context = analysis_output._build_access_point_score_context(
                _build_metrics(),
                _build_displays(),
                active_reason_codes=set(FACTOR_WEIGHTS.keys()),
                normalized_factor_weights={},
            )
        self.assertAlmostEqual(context.total_score, UNCERTAINTY_PENALTY_MAX, places=6)
        self.assertAlmostEqual(context.investigation_score, 28.0, places=6)
        self.assertLess(context.investigation_score, 32.32)
        self.assertLessEqual(context.investigation_score, context.total_score + 30.0)

    def test_normalized_factor_weights_sum_to_94_for_active_codes(self) -> None:
        _, active_reason_codes, normalized_factor_weights = _resolve_access_point_weight_context(list(FACTOR_WEIGHTS.keys()))
        normalized_sum = sum(float(normalized_factor_weights[code]) for code in active_reason_codes)
        self.assertAlmostEqual(normalized_sum, 94.0, places=6)

