from __future__ import annotations

from typing import Any

from app.services.charting import build_empty_chart_bundle as _empty_chart_bundle
from app.services.executive_brief import empty_executive_brief

from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS, SCENARIO_FORECAST_DESCRIPTION
from .data import (
    _build_forecasting_table_options,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
    _table_selection_label,
)
from .payloads import _empty_forecasting_data
from .utils import (
    _format_float_for_input,
    _format_number,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
)


def _build_pending_decision_support_payload(
    *,
    table_options: list[dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
    feature_cards: list[dict[str, str]],
    message: str,
) -> dict[str, Any]:
    risk_prediction = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=forecast_days,
        temperature=temperature,
        history_window=history_window,
    )["risk_prediction"]
    risk_prediction["feature_cards"] = feature_cards
    risk_prediction["top_territory_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    risk_prediction["top_territory_explanation"] = message
    risk_prediction["top_territory_confidence_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    risk_prediction["top_territory_confidence_score_display"] = "..."
    risk_prediction["top_territory_confidence_note"] = message
    risk_prediction["notes"] = [message]
    risk_prediction["quality_passport"]["confidence_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    risk_prediction["quality_passport"]["confidence_score_display"] = "..."
    risk_prediction["quality_passport"]["confidence_tone"] = "sand"
    risk_prediction["quality_passport"]["validation_label"] = "\u0424\u043e\u043d\u043e\u0432\u0430\u044f \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0430"
    risk_prediction["quality_passport"]["validation_summary"] = message
    risk_prediction["quality_passport"]["reliability_notes"] = [message]
    risk_prediction["weight_profile"]["status_label"] = "\u0424\u043e\u043d\u043e\u0432\u0430\u044f \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0430"
    risk_prediction["weight_profile"]["status_tone"] = "sand"
    risk_prediction["weight_profile"]["notes"] = [message]
    risk_prediction["historical_validation"]["status_label"] = "\u0424\u043e\u043d\u043e\u0432\u0430\u044f \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0430"
    risk_prediction["historical_validation"]["status_tone"] = "sand"
    risk_prediction["historical_validation"]["summary"] = message
    risk_prediction["historical_validation"]["notes"] = [message]
    return risk_prediction


def _build_pending_executive_brief(message: str) -> dict[str, Any]:
    executive_brief = empty_executive_brief()
    executive_brief["lead"] = message
    executive_brief["top_territory_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    executive_brief["priority_reason"] = message
    executive_brief["why_value"] = "\u0418\u0434\u0435\u0442 \u0444\u043e\u043d\u043e\u0432\u044b\u0439 \u0440\u0430\u0441\u0447\u0435\u0442"
    executive_brief["why_meta"] = "\u041f\u0440\u0438\u0447\u0438\u043d\u0430 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u0430 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0438 \u0431\u043b\u043e\u043a\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439."
    executive_brief["action_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    executive_brief["action_detail"] = "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438 \u043f\u043e\u044f\u0432\u044f\u0442\u0441\u044f \u0441\u0440\u0430\u0437\u0443 \u043f\u043e\u0441\u043b\u0435 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0438\u044f \u0444\u043e\u043d\u043e\u0432\u043e\u0433\u043e \u0440\u0430\u0441\u0447\u0435\u0442\u0430."
    executive_brief["confidence_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    executive_brief["confidence_score_display"] = "..."
    executive_brief["confidence_tone"] = "sand"
    executive_brief["confidence_summary"] = "\u041f\u0430\u0441\u043f\u043e\u0440\u0442 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0438 \u0434\u043e\u0432\u0435\u0440\u0438\u0435 \u043a \u0434\u0430\u043d\u043d\u044b\u043c \u0434\u043e\u0433\u0440\u0443\u0436\u0430\u044e\u0442\u0441\u044f \u0444\u043e\u043d\u043e\u043c."
    executive_brief["cards"] = [
        {
            "label": "\u041a\u0443\u0434\u0430 \u0441\u043c\u043e\u0442\u0440\u0435\u0442\u044c \u0441\u043d\u0430\u0447\u0430\u043b\u0430",
            "value": "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f",
            "meta": message,
            "tone": "sky",
        },
        {
            "label": "\u041f\u043e\u0447\u0435\u043c\u0443 \u0438\u043c\u0435\u043d\u043d\u043e \u0441\u044e\u0434\u0430",
            "value": "\u0418\u0434\u0435\u0442 \u0444\u043e\u043d\u043e\u0432\u044b\u0439 \u0440\u0430\u0441\u0447\u0435\u0442",
            "meta": "\u041f\u0440\u0438\u0447\u0438\u043d\u0430 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u0430 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0438 \u0431\u043b\u043e\u043a\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439.",
            "tone": "sand",
        },
        {
            "label": "\u041d\u0430\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u043c\u043e\u0436\u043d\u043e \u0434\u043e\u0432\u0435\u0440\u044f\u0442\u044c",
            "value": "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f",
            "meta": "\u041f\u0430\u0441\u043f\u043e\u0440\u0442 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0434\u043e\u0433\u0440\u0443\u0436\u0430\u0435\u0442\u0441\u044f \u0444\u043e\u043d\u043e\u043c.",
            "tone": "sand",
        },
        {
            "label": "\u0427\u0442\u043e \u0441\u0434\u0435\u043b\u0430\u0442\u044c \u043f\u0435\u0440\u0432\u044b\u043c",
            "value": "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f",
            "meta": "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438 \u043f\u043e\u044f\u0432\u044f\u0442\u0441\u044f \u0441\u0440\u0430\u0437\u0443 \u043f\u043e\u0441\u043b\u0435 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0438\u044f \u0444\u043e\u043d\u043e\u0432\u043e\u0433\u043e \u0440\u0430\u0441\u0447\u0435\u0442\u0430.",
            "tone": "forest",
        },
    ]
    executive_brief["notes"] = [message]
    executive_brief["export_excerpt"] = message
    return executive_brief


def _build_slice_label(
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
) -> str:
    slice_parts = []
    if selected_district != "all":
        slice_parts.append(f"\u0440\u0430\u0439\u043e\u043d: {selected_district}")
    if selected_cause != "all":
        slice_parts.append(f"\u043f\u0440\u0438\u0447\u0438\u043d\u0430: {selected_cause}")
    if selected_object_category != "all":
        slice_parts.append(f"\u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f: {selected_object_category}")
    return " | ".join(slice_parts) if slice_parts else "\u0412\u0441\u0435 \u043f\u043e\u0436\u0430\u0440\u044b \u0432\u044b\u0431\u0440\u0430\u043d\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438"


def _normalize_shell_filter_value(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized or "all"


def _build_shell_filter_options(selected_value: str, all_label: str) -> list[dict[str, str]]:
    normalized = _normalize_shell_filter_value(selected_value)
    if normalized == "all":
        return [{"value": "all", "label": all_label}]
    return [
        {"value": normalized, "label": normalized},
        {"value": "all", "label": all_label},
    ]


def _build_metadata_loading_message() -> str:
    return (
        "\u0421\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043e\u0442\u043a\u0440\u044b\u0442\u0430 \u0432 \u0431\u044b\u0441\u0442\u0440\u043e\u043c \u0440\u0435\u0436\u0438\u043c\u0435: \u0441\u043d\u0430\u0447\u0430\u043b\u0430 \u0434\u043e\u0433\u0440\u0443\u0436\u0430\u0435\u043c \u0444\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438, "
        "\u0437\u0430\u0442\u0435\u043c \u0437\u0430\u043f\u0443\u0441\u043a\u0430\u0435\u043c \u0431\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437, \u0430 \u043f\u043e\u0441\u043b\u0435 \u043d\u0435\u0433\u043e \u0431\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439."
    )


def _build_base_forecast_loading_message() -> str:
    return "\u0424\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438 \u0437\u0430\u0433\u0440\u0443\u0436\u0430\u044e\u0442\u0441\u044f. \u0411\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u0441\u0442\u0430\u0440\u0442\u0443\u0435\u0442 \u0441\u0440\u0430\u0437\u0443 \u043f\u043e\u0441\u043b\u0435 \u044d\u0442\u043e\u0433\u043e \u044d\u0442\u0430\u043f\u0430."


def _build_decision_support_followup_message() -> str:
    return (
        "\u0422\u0440\u0435\u0442\u0438\u0439 \u044d\u0442\u0430\u043f \u0437\u0430\u043f\u0443\u0441\u043a\u0430\u0435\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430: \u0434\u043e\u0433\u0440\u0443\u0436\u0430\u044e\u0442\u0441\u044f \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u044b \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439, "
        "\u043f\u0430\u0441\u043f\u043e\u0440\u0442 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0438 \u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438."
    )


def _build_shell_risk_prediction(
    *,
    table_options: list[dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
    feature_cards: list[dict[str, str]],
    message: str,
) -> dict[str, Any]:
    followup_message = (
        "\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u044b \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439, \u043f\u0430\u0441\u043f\u043e\u0440\u0442 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430 \u0438 \u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438 \u0437\u0430\u043f\u0443\u0441\u0442\u044f\u0442\u0441\u044f \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u043f\u043e\u0441\u043b\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430."
    )
    risk_prediction = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=forecast_days,
        temperature=temperature,
        history_window=history_window,
    )["risk_prediction"]
    risk_prediction["feature_cards"] = feature_cards
    risk_prediction["top_territory_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u043c \u043f\u0440\u043e\u0433\u043d\u043e\u0437"
    risk_prediction["top_territory_explanation"] = message
    risk_prediction["top_territory_confidence_label"] = "\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435"
    risk_prediction["top_territory_confidence_score_display"] = "..."
    risk_prediction["top_territory_confidence_note"] = followup_message
    risk_prediction["notes"] = [message, followup_message]
    risk_prediction["quality_passport"]["confidence_label"] = "\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435"
    risk_prediction["quality_passport"]["confidence_score_display"] = "..."
    risk_prediction["quality_passport"]["confidence_tone"] = "sand"
    risk_prediction["quality_passport"]["validation_label"] = "\u0416\u0434\u0435\u0442 \u0431\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437"
    risk_prediction["quality_passport"]["validation_summary"] = followup_message
    risk_prediction["quality_passport"]["reliability_notes"] = [followup_message]
    risk_prediction["weight_profile"]["status_label"] = "\u0416\u0434\u0435\u0442 \u0431\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437"
    risk_prediction["weight_profile"]["status_tone"] = "sand"
    risk_prediction["weight_profile"]["description"] = (
        "\u041f\u0440\u043e\u0444\u0438\u043b\u044c \u0432\u0435\u0441\u043e\u0432 \u0441\u0442\u0430\u043d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u043f\u043e\u0441\u043b\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430 \u0438 \u043f\u043e\u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0434\u043e\u0433\u0440\u0443\u0437\u043a\u0438 \u0431\u043b\u043e\u043a\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439."
    )
    risk_prediction["weight_profile"]["notes"] = [followup_message]
    risk_prediction["weight_profile"]["calibration_notes"] = [followup_message]
    risk_prediction["historical_validation"]["status_label"] = "\u0416\u0434\u0435\u0442 \u0431\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437"
    risk_prediction["historical_validation"]["status_tone"] = "sand"
    risk_prediction["historical_validation"]["summary"] = followup_message
    risk_prediction["historical_validation"]["notes"] = [followup_message]
    risk_prediction["geo_summary"]["compact_message"] = "\u041a\u0430\u0440\u0442\u0430 \u0440\u0438\u0441\u043a\u0430 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430."
    risk_prediction["geo_summary"]["coverage_display"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    risk_prediction["geo_summary"]["top_zone_label"] = "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f"
    risk_prediction["geo_summary"]["top_risk_display"] = "..."
    risk_prediction["geo_summary"]["hotspots_count_display"] = "..."
    risk_prediction["geo_summary"]["top_explanation"] = followup_message
    return risk_prediction


def _build_forecasting_shell_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    *,
    table_options_builder=_build_forecasting_table_options,
    selection_resolver=_resolve_forecasting_selection,
    source_tables_resolver=_selected_source_tables,
    source_notes_resolver=_selected_source_table_notes,
    forecast_days_parser=_parse_forecast_days,
    history_window_parser=_parse_history_window,
) -> dict[str, Any]:
    table_options = table_options_builder()
    selected_table = selection_resolver(table_options, table_name)
    source_tables = source_tables_resolver(table_options, selected_table)
    source_table_notes = source_notes_resolver(table_options, selected_table)
    days_ahead = forecast_days_parser(forecast_days)
    selected_history_window = history_window_parser(history_window)
    temperature_value = _parse_float(temperature)
    requested_district = _normalize_shell_filter_value(district)
    requested_cause = _normalize_shell_filter_value(cause)
    requested_object_category = _normalize_shell_filter_value(object_category)

    shell_data = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=days_ahead,
        temperature=temperature,
        history_window=selected_history_window,
    )
    if not source_tables:
        shell_data["notes"].extend(source_table_notes)
        shell_data["notes"].append("\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0445 \u0442\u0430\u0431\u043b\u0438\u0446 \u0434\u043b\u044f \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f.")
        return shell_data

    metadata_message = _build_metadata_loading_message()
    base_loading_message = _build_base_forecast_loading_message()
    followup_message = _build_decision_support_followup_message()

    shell_data["bootstrap_mode"] = "deferred"
    shell_data["loading"] = True
    shell_data["deferred"] = True
    shell_data["metadata_pending"] = True
    shell_data["metadata_ready"] = False
    shell_data["metadata_error"] = False
    shell_data["metadata_status_message"] = metadata_message
    shell_data["base_forecast_pending"] = True
    shell_data["base_forecast_ready"] = False
    shell_data["loading_status_message"] = base_loading_message
    shell_data["decision_support_pending"] = False
    shell_data["decision_support_ready"] = False
    shell_data["decision_support_error"] = False
    shell_data["decision_support_status_message"] = ""
    shell_data["model_description"] = SCENARIO_FORECAST_DESCRIPTION
    shell_data["summary"].update(
        {
            "selected_table_label": _table_selection_label(selected_table),
            "slice_label": _build_slice_label(requested_district, requested_cause, requested_object_category),
            "history_period_label": "\u0418\u0441\u0442\u043e\u0440\u0438\u044f \u0437\u0430\u0433\u0440\u0443\u0436\u0430\u0435\u0442\u0441\u044f",
            "history_window_label": _history_window_label(selected_history_window),
            "fires_count_display": "...",
            "history_days_display": "...",
            "active_days_display": "...",
            "last_observed_date": "-",
            "forecast_days_display": str(days_ahead),
            "predicted_total_display": "...",
            "predicted_average_display": "...",
            "average_probability_display": "...",
            "historical_average_display": "...",
            "recent_average_display": "...",
            "forecast_vs_recent_display": "...",
            "forecast_vs_recent_label": "\u041f\u043e\u0434\u0433\u043e\u0442\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f",
            "active_days_share_display": "...",
            "peak_forecast_day_display": "-",
            "peak_forecast_value_display": "...",
            "peak_forecast_probability_display": "...",
            "temperature_scenario_display": f"{_format_number(temperature_value)} \xb0C" if temperature_value is not None else "\u0418\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u0441\u0435\u0437\u043e\u043d\u043d\u043e\u0441\u0442\u044c",
        }
    )
    shell_data["quality_assessment"]["subtitle"] = (
        "\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0434\u043e\u0433\u0440\u0443\u0436\u0430\u0435\u043c \u0444\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438, \u0437\u0430\u0442\u0435\u043c \u0441\u0447\u0438\u0442\u0430\u0435\u043c \u0431\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u0438 \u0442\u043e\u043b\u044c\u043a\u043e \u043f\u043e\u0441\u043b\u0435 \u044d\u0442\u043e\u0433\u043e \u0441\u043e\u0431\u0438\u0440\u0430\u0435\u043c \u043c\u0435\u0442\u0440\u0438\u043a\u0438 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u0430."
    )
    shell_data["quality_assessment"]["dissertation_points"] = [metadata_message, followup_message]
    shell_data["risk_prediction"] = _build_shell_risk_prediction(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=days_ahead,
        temperature=temperature,
        history_window=selected_history_window,
        feature_cards=[],
        message=metadata_message,
    )
    shell_data["executive_brief"] = _build_pending_executive_brief(metadata_message)
    shell_data["executive_brief"]["notes"] = source_table_notes + [metadata_message, base_loading_message, followup_message]
    shell_data["executive_brief"]["export_excerpt"] = metadata_message
    shell_data["charts"] = {
        "daily": _empty_chart_bundle("\u0427\u0442\u043e \u0431\u044b\u043b\u043e \u0438 \u0447\u0442\u043e \u043e\u0436\u0438\u0434\u0430\u0435\u0442\u0441\u044f", "\u0411\u0430\u0437\u043e\u0432\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0444\u0438\u043b\u044c\u0442\u0440\u043e\u0432 \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u043e\u0432."),
        "breakdown": _empty_chart_bundle("\u0421\u0446\u0435\u043d\u0430\u0440\u043d\u0430\u044f \u0432\u0435\u0440\u043e\u044f\u0442\u043d\u043e\u0441\u0442\u044c \u043f\u043e\u0436\u0430\u0440\u0430 \u043f\u043e \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0438\u043c \u0434\u043d\u044f\u043c", "\u0420\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u0435 \u043f\u043e \u0434\u043d\u044f\u043c \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043d\u0430 \u0432\u0442\u043e\u0440\u043e\u043c \u044d\u0442\u0430\u043f\u0435 \u043f\u043e\u0441\u043b\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0444\u0438\u043b\u044c\u0442\u0440\u043e\u0432 \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u043e\u0432."),
        "weekday": _empty_chart_bundle("\u0412 \u043a\u0430\u043a\u0438\u0435 \u0434\u043d\u0438 \u043d\u0435\u0434\u0435\u043b\u0438 \u043f\u043e\u0436\u0430\u0440\u044b \u0441\u043b\u0443\u0447\u0430\u044e\u0442\u0441\u044f \u0447\u0430\u0449\u0435", "\u041d\u0435\u0434\u0435\u043b\u044c\u043d\u044b\u0439 \u043f\u0440\u043e\u0444\u0438\u043b\u044c \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0431\u0430\u0437\u043e\u0432\u043e\u0433\u043e \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430."),
        "geo": _empty_chart_bundle("\u041a\u0430\u0440\u0442\u0430 \u0431\u043b\u043e\u043a\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439", "\u041a\u0430\u0440\u0442\u0430 \u0440\u0438\u0441\u043a\u0430 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043d\u0430 \u0442\u0440\u0435\u0442\u044c\u0435\u043c \u044d\u0442\u0430\u043f\u0435 \u043f\u043e\u0441\u043b\u0435 \u0431\u043b\u043e\u043a\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439."),
    }
    shell_data["notes"] = source_table_notes + [metadata_message, base_loading_message, followup_message]
    shell_data["filters"] = {
        "table_name": selected_table,
        "district": requested_district,
        "cause": requested_cause,
        "object_category": requested_object_category,
        "temperature": temperature if temperature_value is None else _format_float_for_input(temperature_value),
        "forecast_days": str(days_ahead),
        "history_window": selected_history_window,
        "available_tables": table_options,
        "available_districts": _build_shell_filter_options(requested_district, "\u0412\u0441\u0435 \u0440\u0430\u0439\u043e\u043d\u044b"),
        "available_causes": _build_shell_filter_options(requested_cause, "\u0412\u0441\u0435 \u043f\u0440\u0438\u0447\u0438\u043d\u044b"),
        "available_object_categories": _build_shell_filter_options(requested_object_category, "\u0412\u0441\u0435 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0438"),
        "available_forecast_days": [{"value": str(option), "label": f"{option} \u0434\u043d\u0435\u0439"} for option in FORECAST_DAY_OPTIONS],
        "available_history_windows": HISTORY_WINDOW_OPTIONS,
    }
    return shell_data
