from app.services.ml_model.ml_model_result_types import EventMetrics


def test_event_metrics_coerce_none_returns_defaults() -> None:
    metrics = EventMetrics.coerce(None)

    assert metrics.available is False
    assert metrics.rows_used == 0
    assert metrics.selection_rule == ""
    assert metrics.selected_metrics.brier_score is None
    assert metrics.baseline_metrics.roc_auc is None
    assert metrics.heuristic_metrics.f1 is None


def test_event_metrics_coerce_uses_legacy_flat_keys_when_nested_missing() -> None:
    metrics = EventMetrics.coerce(
        {
            "brier_score": "0.12",
            "roc_auc": "0.83",
            "f1": "0.61",
            "log_loss": "0.49",
            "baseline_brier_score": "0.22",
            "heuristic_roc_auc": "0.72",
        }
    )

    assert metrics.selected_metrics.brier_score == 0.12
    assert metrics.selected_metrics.roc_auc == 0.83
    assert metrics.selected_metrics.f1 == 0.61
    assert metrics.selected_metrics.log_loss == 0.49
    assert metrics.baseline_metrics.brier_score == 0.22
    assert metrics.heuristic_metrics.roc_auc == 0.72


def test_event_metrics_coerce_prefers_nested_metrics_over_flat_legacy_values() -> None:
    metrics = EventMetrics.coerce(
        {
            "brier_score": 0.99,
            "selected_metrics": {"brier_score": "0.11"},
            "baseline_roc_auc": 0.99,
            "baseline_metrics": {"roc_auc": "0.44"},
        }
    )

    assert metrics.selected_metrics.brier_score == 0.11
    assert metrics.baseline_metrics.roc_auc == 0.44


def test_event_metrics_coerce_invalid_numeric_strings_fallback_to_none() -> None:
    metrics = EventMetrics.coerce(
        {
            "selected_metrics": {
                "brier_score": "nan-value",
                "roc_auc": "oops",
                "f1": "",
                "log_loss": "bad",
            },
            "event_rate": "not-a-number",
        }
    )

    assert metrics.selected_metrics.brier_score is None
    assert metrics.selected_metrics.roc_auc is None
    assert metrics.selected_metrics.f1 is None
    assert metrics.selected_metrics.log_loss is None
    assert metrics.event_rate is None


def test_event_metrics_coerce_missing_keys_in_row_dicts_keeps_row_defaults() -> None:
    metrics = EventMetrics.coerce({"comparison_rows": [{"method_key": "baseline"}, {}]})

    assert len(metrics.comparison_rows) == 2
    assert metrics.comparison_rows[0].method_key == "baseline"
    assert metrics.comparison_rows[0].brier_score is None
    assert metrics.comparison_rows[1].method_key == ""
    assert metrics.comparison_rows[1].is_selected is False
