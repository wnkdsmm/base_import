from __future__ import annotations

from statistics import mean
from typing import Any, Dict, List

from app.services.model_quality import compute_count_metrics

from .data import _build_forecast_rows
from .utils import _format_integer, _format_number, _format_signed_percent


def _scenario_baseline_expected_count(history: List[dict[str, Any]], target_date: Any) -> float:
    if not history:
        return 0.0
    history_counts = [float(item["count"]) for item in history]
    recent_mean = mean(history_counts[-28:]) if history_counts else 0.0
    same_weekday = [float(item["count"]) for item in history if item["date"].weekday() == target_date.weekday()]
    if len(same_weekday) >= 3:
        return max(0.0, 0.6 * mean(same_weekday[-8:]) + 0.4 * recent_mean)
    return max(0.0, recent_mean)


def _run_scenario_backtesting(daily_history: List[dict[str, Any]]) -> dict[str, Any]:
    if len(daily_history) < 30:
        return {
            "is_ready": False,
            "message": "\u0414\u043b\u044f \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u043f\u043e\u043a\u0430 \u043c\u0430\u043b\u043e \u043d\u0435\u043f\u0440\u0435\u0440\u044b\u0432\u043d\u043e\u0439 \u0434\u043d\u0435\u0432\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438: \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438 \u0432\u043a\u043b\u044e\u0447\u0430\u0435\u0442\u0441\u044f \u043f\u0440\u0438\u043c\u0435\u0440\u043d\u043e \u043e\u0442 30 \u0434\u043d\u0435\u0439 \u0440\u044f\u0434\u0430.",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": 0, "validation_horizon_days": 1},
        }

    min_train_days = min(28, max(14, len(daily_history) // 2))
    available_points = len(daily_history) - min_train_days
    if available_points < 8:
        return {
            "is_ready": False,
            "message": "\u0414\u043b\u044f \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u043f\u043e\u043a\u0430 \u043d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u043e\u0434\u043d\u043e\u0448\u0430\u0433\u043e\u0432\u044b\u0445 \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u0445 \u043e\u043a\u043e\u043d: \u043d\u0443\u0436\u043d\u043e \u0445\u043e\u0442\u044f \u0431\u044b 8 \u043e\u043a\u043e\u043d \u0434\u043b\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438.",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": min_train_days, "validation_horizon_days": 1},
        }

    start_index = len(daily_history) - min(45, available_points)
    rows: List[dict[str, Any]] = []
    for index in range(start_index, len(daily_history)):
        train_history = daily_history[:index]
        actual_row = daily_history[index]
        if not train_history:
            continue
        forecast_row = _build_forecast_rows(train_history, 1, None)
        if not forecast_row:
            continue
        point_forecast = forecast_row[0]
        baseline_count = _scenario_baseline_expected_count(train_history, actual_row["date"])
        rows.append(
            {
                "date": actual_row["date"].isoformat(),
                "actual_count": float(actual_row["count"]),
                "predicted_count": float(point_forecast.get("forecast_value", 0.0)),
                "baseline_count": float(baseline_count),
            }
        )

    if len(rows) < 8:
        return {
            "is_ready": False,
            "message": "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0431\u0440\u0430\u0442\u044c \u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u0445 \u043f\u0440\u043e\u0432\u0435\u0440\u043e\u043a \u0434\u043b\u044f \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430.",
            "rows": rows,
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": len(rows), "min_train_days": min_train_days, "validation_horizon_days": 1},
        }

    actuals = [row["actual_count"] for row in rows]
    predictions = [row["predicted_count"] for row in rows]
    baseline_predictions = [row["baseline_count"] for row in rows]
    baseline_metrics = compute_count_metrics(actuals, baseline_predictions)
    model_metrics = compute_count_metrics(actuals, predictions, baseline_metrics)
    return {
        "is_ready": True,
        "message": "",
        "rows": rows,
        "model_metrics": model_metrics,
        "baseline_metrics": baseline_metrics,
        "overview": {"folds": len(rows), "min_train_days": min_train_days, "validation_horizon_days": 1},
    }


def _empty_forecast_quality_assessment() -> dict[str, Any]:
    return {
        "ready": False,
        "title": "\u041e\u0446\u0435\u043d\u043a\u0430 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430",
        "subtitle": "\u041f\u043e\u0441\u043b\u0435 \u043d\u0430\u043a\u043e\u043f\u043b\u0435\u043d\u0438\u044f \u0438\u0441\u0442\u043e\u0440\u0438\u0438 \u0437\u0434\u0435\u0441\u044c \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0438\u043c\u0435\u043d\u043d\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u043f\u043e \u0434\u043d\u044f\u043c \u0438 \u0441\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u0435 \u0441 \u0431\u0430\u0437\u043e\u0432\u043e\u0439 \u043c\u043e\u0434\u0435\u043b\u044c\u044e.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["\u0418\u0441\u0442\u043e\u0440\u0438\u0438 \u043f\u043e\u043a\u0430 \u043d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e, \u0447\u0442\u043e\u0431\u044b \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u044c \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u043e \u044d\u0432\u0440\u0438\u0441\u0442\u0438\u0447\u0435\u0441\u043a\u043e\u0433\u043e \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u0447\u0435\u0440\u0435\u0437 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0443 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438."],
    }


def _build_scenario_quality_assessment(backtest: dict[str, Any]) -> dict[str, Any]:
    if not backtest.get("is_ready"):
        payload = _empty_forecast_quality_assessment()
        message = backtest.get("message")
        if message:
            payload["dissertation_points"] = [message]
        return payload

    model_metrics = backtest.get("model_metrics", {})
    baseline_metrics = backtest.get("baseline_metrics", {})
    overview = backtest.get("overview", {})
    comparison_rows = [
        {
            "method_label": "\u0421\u0435\u0437\u043e\u043d\u043d\u0430\u044f \u0431\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c",
            "role_label": "\u0411\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c",
            "mae_display": _format_number(baseline_metrics.get("mae")),
            "rmse_display": _format_number(baseline_metrics.get("rmse")),
            "smape_display": f"{_format_number(baseline_metrics.get('smape'))}%",
            "selection_label": "\u041e\u043f\u043e\u0440\u043d\u0430\u044f \u043b\u0438\u043d\u0438\u044f",
            "mae_delta_display": "0%",
        },
        {
            "method_label": "\u0421\u0446\u0435\u043d\u0430\u0440\u043d\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437",
            "role_label": "\u042d\u0432\u0440\u0438\u0441\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c",
            "mae_display": _format_number(model_metrics.get("mae")),
            "rmse_display": _format_number(model_metrics.get("rmse")),
            "smape_display": f"{_format_number(model_metrics.get('smape'))}%",
            "selection_label": "\u0420\u0430\u0431\u043e\u0447\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c",
            "mae_delta_display": _format_signed_percent(model_metrics.get("mae_delta_vs_baseline")) if model_metrics.get("mae_delta_vs_baseline") is not None else "\u2014",
        },
    ]
    return {
        "ready": True,
        "title": "\u041e\u0446\u0435\u043d\u043a\u0430 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430",
        "subtitle": "\u0421\u0446\u0435\u043d\u0430\u0440\u043d\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u043f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u0442\u0441\u044f \u043f\u043e \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u043c \u043e\u043a\u043d\u0430\u043c \u0431\u0435\u0437 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u0438\u044f \u0431\u0443\u0434\u0443\u0449\u0438\u0445 \u043d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0439. \u042d\u0442\u043e \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043a\u0430\u043b\u0435\u043d\u0434\u0430\u0440\u044f \u043f\u043e \u0434\u043d\u044f\u043c, \u0430 \u043d\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u0430.",
        "metric_cards": [
            {"label": "MAE", "value": _format_number(model_metrics.get("mae")), "meta": f"\u0431\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c: {_format_number(baseline_metrics.get('mae'))}"},
            {"label": "RMSE", "value": _format_number(model_metrics.get("rmse")), "meta": f"\u0431\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c: {_format_number(baseline_metrics.get('rmse'))}"},
            {"label": "SMAPE", "value": f"{_format_number(model_metrics.get('smape'))}%", "meta": f"\u0431\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c: {_format_number(baseline_metrics.get('smape'))}%"},
            {"label": "MAE \u043a \u0431\u0430\u0437\u043e\u0432\u043e\u0439 \u043c\u043e\u0434\u0435\u043b\u0438", "value": _format_signed_percent(model_metrics.get("mae_delta_vs_baseline")) if model_metrics.get("mae_delta_vs_baseline") is not None else "\u2014", "meta": "\u043e\u0442\u0440\u0438\u0446\u0430\u0442\u0435\u043b\u044c\u043d\u043e\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043b\u0443\u0447\u0448\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0439 \u043c\u043e\u0434\u0435\u043b\u0438"},
        ],
        "methodology_items": [
            {"label": "\u0421\u0445\u0435\u043c\u0430 \u0432\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u0438", "value": "\u0421\u043a\u043e\u043b\u044c\u0437\u044f\u0449\u0430\u044f \u043e\u0434\u043d\u043e\u0448\u0430\u0433\u043e\u0432\u0430\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438", "meta": "\u043a\u0430\u0436\u0434\u043e\u0435 \u043e\u043a\u043d\u043e \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u0442 \u0442\u043e\u043b\u044c\u043a\u043e \u043f\u0440\u043e\u0448\u043b\u0443\u044e \u0438\u0441\u0442\u043e\u0440\u0438\u044e"},
            {"label": "\u041e\u043a\u043e\u043d \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438", "value": _format_integer(overview.get("folds") or 0), "meta": "\u043e\u0434\u043d\u043e\u0448\u0430\u0433\u043e\u0432\u044b\u0435 \u043e\u043a\u043d\u0430"},
            {"label": "\u041c\u0438\u043d\u0438\u043c\u0443\u043c \u043e\u0431\u0443\u0447\u0430\u044e\u0449\u0435\u0433\u043e \u043e\u043a\u043d\u0430", "value": _format_integer(overview.get("min_train_days") or 0), "meta": "\u0434\u043d\u0435\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438 \u043d\u0430 \u043e\u043a\u043d\u043e"},
            {"label": "\u0413\u043e\u0440\u0438\u0437\u043e\u043d\u0442", "value": _format_integer(overview.get("validation_horizon_days") or 1), "meta": "\u0434\u0435\u043d\u044c \u0432\u043f\u0435\u0440\u0451\u0434"},
        ],
        "comparison_rows": comparison_rows,
        "dissertation_points": [
            f"\u041a\u0430\u0447\u0435\u0441\u0442\u0432\u043e \u044d\u0432\u0440\u0438\u0441\u0442\u0438\u0447\u0435\u0441\u043a\u043e\u0433\u043e \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u043e\u0446\u0435\u043d\u0435\u043d\u043e \u0447\u0435\u0440\u0435\u0437 \u0441\u043a\u043e\u043b\u044c\u0437\u044f\u0449\u0443\u044e \u043e\u0434\u043d\u043e\u0448\u0430\u0433\u043e\u0432\u0443\u044e \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0443 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438 \u043d\u0430 {_format_integer(overview.get('folds') or 0)} \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u0445 \u043e\u043a\u043d\u0430\u0445.",
            f"\u0421\u0446\u0435\u043d\u0430\u0440\u043d\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u043f\u043e\u043a\u0430\u0437\u0430\u043b MAE {_format_number(model_metrics.get('mae'))}, RMSE {_format_number(model_metrics.get('rmse'))} \u0438 SMAPE {_format_number(model_metrics.get('smape'))}%.",
            f"\u0421\u0435\u0437\u043e\u043d\u043d\u0430\u044f \u0431\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c \u043d\u0430 \u0442\u0435\u0445 \u0436\u0435 \u043e\u043a\u043d\u0430\u0445 \u0434\u0430\u043b\u0430 MAE {_format_number(baseline_metrics.get('mae'))}, RMSE {_format_number(baseline_metrics.get('rmse'))} \u0438 SMAPE {_format_number(baseline_metrics.get('smape'))}%.",
            f"\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 MAE \u043e\u0442\u043d\u043e\u0441\u0438\u0442\u0435\u043b\u044c\u043d\u043e \u0431\u0430\u0437\u043e\u0432\u043e\u0439 \u043c\u043e\u0434\u0435\u043b\u0438 \u0441\u043e\u0441\u0442\u0430\u0432\u0438\u043b\u043e {_format_signed_percent(model_metrics.get('mae_delta_vs_baseline')) if model_metrics.get('mae_delta_vs_baseline') is not None else '\u2014'}, \u0447\u0442\u043e \u043f\u043e\u0437\u0432\u043e\u043b\u044f\u0435\u0442 \u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u0435\u043d\u043d\u043e \u0441\u0440\u0430\u0432\u043d\u0438\u0432\u0430\u0442\u044c \u0441\u0446\u0435\u043d\u0430\u0440\u043d\u044b\u0439 \u0431\u043b\u043e\u043a \u0441 \u043e\u043f\u043e\u0440\u043d\u043e\u0439 \u0441\u0435\u0437\u043e\u043d\u043d\u043e\u0439 \u043c\u043e\u0434\u0435\u043b\u044c\u044e.",
        ],
    }
