from __future__ import annotations

import math
import numbers
from datetime import datetime
from typing import Any, Dict, List, Tuple

from app.perf import profiled
from app.runtime_cache import CopyingTtlCache
from app.services.charting import build_empty_chart_bundle as _empty_chart_bundle
from app.services.shared.formatting import _format_datetime
from config.db import engine

from .analysis_output import (
    _build_reason_breakdown,
    _build_score_distribution,
    _build_typology_rows,
    _build_uncertainty_notes,
)
from .analysis_ranking import (
    _build_access_point_rows,
    _build_access_point_rows_from_entity_frame,
    _select_incomplete_points,
    _select_top_points,
)
from .charts import _build_points_scatter_chart
from .constants import ACCESS_POINT_LIMIT_OPTIONS
from .data import (
    _build_access_points_table_options,
    _build_option_catalog,
    _collect_access_point_metadata,
    _parse_limit,
    _resolve_option_value,
    _resolve_selected_table,
    _selected_source_tables,
)
from .features import (
    _build_access_point_candidate_features,
    _build_access_point_feature_options,
    _build_access_point_shell_feature_options,
    _normalize_access_point_feature_columns,
    _resolve_selected_access_point_features,
)
from .point_data import _load_access_point_dataset
from .presentation import (
    _build_notes,
    _build_summary,
    _build_summary_cards,
    _build_top_point_lead,
    _empty_access_points_data,
    _selection_label,
)

_ACCESS_POINTS_CACHE = CopyingTtlCache(ttl_seconds=120.0)


def clear_access_points_cache() -> None:
    _ACCESS_POINTS_CACHE.clear()


def _normalize_cache_value(value: str) -> str:
    return str(value or "").strip()


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return _sanitize_json_payload(value.item())
        except Exception:
            pass
    try:
        if value != value:
            return None
    except Exception:
        pass
    try:
        text = str(value).strip().lower()
    except Exception:
        text = ""
    if text in {"nan", "nat", "<na>", "inf", "-inf"}:
        return None
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return numeric
        except Exception:
            return None
    return value


def _build_access_points_cache_key(
    *,
    selected_table: str,
    source_tables: List[str],
    district: str,
    year: str,
    limit: int,
    feature_columns: List[str],
) -> Tuple[str, ...]:
    return (
        "v6",
        selected_table,
        *tuple(source_tables),
        _normalize_cache_value(district),
        _normalize_cache_value(year),
        str(limit),
        *tuple(feature_columns),
    )


def _build_limit_options() -> List[Dict[str, str]]:
    return [{"value": str(value), "label": f"Top {value}"} for value in ACCESS_POINT_LIMIT_OPTIONS]


def _empty_access_points_charts(message: str) -> Dict[str, Any]:
    return {
        "scatter": _empty_chart_bundle(
            "Проблемные точки на проекции доступности и последствий",
            message,
        )
    }


def _build_access_points_request_state(
    *,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: List[str] | None = None,
) -> Dict[str, Any]:
    table_options = _build_access_points_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    parsed_limit = _parse_limit(limit)
    normalized_feature_columns = _normalize_access_point_feature_columns(feature_columns)
    cache_key = _build_access_points_cache_key(
        selected_table=selected_table,
        source_tables=source_tables,
        district=district,
        year=year,
        limit=parsed_limit,
        feature_columns=normalized_feature_columns,
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "source_tables": source_tables,
        "selected_district": str(district or "all").strip() or "all",
        "selected_year": str(year or "all").strip() or "all",
        "limit": parsed_limit,
        "feature_columns": normalized_feature_columns,
        "cache_key": cache_key,
    }


def _shell_options(current_value: str, all_label: str) -> List[Dict[str, str]]:
    if current_value == "all":
        return [{"value": "all", "label": all_label}]
    return [
        {"value": "all", "label": all_label},
        {"value": current_value, "label": current_value},
    ]


@profiled("access_points.shell", engine=engine)
def get_access_points_shell_context(
    *,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: List[str] | None = None,
) -> Dict[str, Any]:
    request_state = _build_access_points_request_state(
        table_name=table_name,
        district=district,
        year=year,
        limit=limit,
        feature_columns=feature_columns,
    )
    cached = _ACCESS_POINTS_CACHE.get(request_state["cache_key"])
    if cached is not None:
        cached = _sanitize_json_payload(cached)
        return {
            "generated_at": _format_datetime(datetime.now()),
            "initial_data": cached,
            "plotly_js": "",
            "has_data": bool(cached.get("filters", {}).get("available_tables")),
        }

    selected_table_label = _selection_label(
        request_state["table_options"],
        request_state["selected_table"],
        "Все таблицы",
    )
    selected_district_label = (
        request_state["selected_district"] if request_state["selected_district"] != "all" else "Все районы"
    )
    selected_year_label = request_state["selected_year"] if request_state["selected_year"] != "all" else "Все годы"

    filters = {
        "table_name": request_state["selected_table"],
        "district": request_state["selected_district"],
        "year": request_state["selected_year"],
        "limit": str(request_state["limit"]),
        "feature_columns": list(request_state["feature_columns"]),
        "available_tables": request_state["table_options"],
        "available_districts": _shell_options(request_state["selected_district"], "Все районы"),
        "available_years": _shell_options(request_state["selected_year"], "Все годы"),
        "available_limits": _build_limit_options(),
        "available_features": _build_access_point_shell_feature_options(request_state["feature_columns"]),
    }
    summary = _build_summary(
        [],
        selected_table_label=selected_table_label,
        selected_district_label=selected_district_label,
        selected_year_label=selected_year_label,
        limit=request_state["limit"],
        total_incidents=0,
        incomplete_points=[],
    )
    initial_data = _empty_access_points_data(
        filters=filters,
        summary=summary,
        notes=[
            "Открыт лёгкий shell-режим страницы. Полный ranking проблемных точек догружается отдельным запросом.",
        ],
        bootstrap_mode="deferred",
    )
    initial_data["charts"] = _empty_access_points_charts("График появится после фонового расчёта проблемных точек.")
    initial_data["loading_status_message"] = "Собираем incidents по точкам, считаем score доступности и объяснения."
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": "",
        "has_data": bool(initial_data.get("filters", {}).get("available_tables")),
    }


@profiled("access_points", engine=engine)
def get_access_points_data(
    *,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: List[str] | None = None,
) -> Dict[str, Any]:
    request_state = _build_access_points_request_state(
        table_name=table_name,
        district=district,
        year=year,
        limit=limit,
        feature_columns=feature_columns,
    )
    cached = _ACCESS_POINTS_CACHE.get(request_state["cache_key"])
    if cached is not None:
        return _sanitize_json_payload(cached)

    filters = {
        "table_name": request_state["selected_table"],
        "district": request_state["selected_district"],
        "year": request_state["selected_year"],
        "limit": str(request_state["limit"]),
        "feature_columns": list(request_state["feature_columns"]),
        "available_tables": request_state["table_options"],
        "available_districts": [{"value": "all", "label": "Все районы"}],
        "available_years": [{"value": "all", "label": "Все годы"}],
        "available_limits": _build_limit_options(),
        "available_features": _build_access_point_shell_feature_options(request_state["feature_columns"]),
    }
    selected_table_label = _selection_label(
        request_state["table_options"],
        request_state["selected_table"],
        "Все таблицы",
    )

    if not request_state["source_tables"]:
        summary = _build_summary(
            [],
            selected_table_label=selected_table_label,
            selected_district_label="Все районы",
            selected_year_label="Все годы",
            limit=request_state["limit"],
            total_incidents=0,
            incomplete_points=[],
        )
        return _ACCESS_POINTS_CACHE.set(
            request_state["cache_key"],
            _sanitize_json_payload({
                **_empty_access_points_data(
                filters=filters,
                summary=summary,
                notes=["Нет доступных таблиц для расчёта проблемных точек."],
                ),
                "charts": _empty_access_points_charts("Нет доступных данных для построения графика."),
            }),
        )

    metadata_items, metadata_notes = _collect_access_point_metadata(request_state["source_tables"])
    option_catalog = _build_option_catalog(
        request_state["source_tables"],
        metadata_items=metadata_items,
    )
    selected_district = _resolve_option_value(
        option_catalog.get("districts") or [{"value": "all", "label": "Все районы"}],
        request_state["selected_district"],
        default="all",
    )
    selected_year_value = _resolve_option_value(
        option_catalog.get("years") or [{"value": "all", "label": "Все годы"}],
        request_state["selected_year"],
        default="all",
    )
    selected_year = None if selected_year_value == "all" else int(selected_year_value)

    filters = {
        "table_name": request_state["selected_table"],
        "district": selected_district,
        "year": selected_year_value,
        "limit": str(request_state["limit"]),
        "feature_columns": list(request_state["feature_columns"]),
        "available_tables": request_state["table_options"],
        "available_districts": option_catalog.get("districts") or [{"value": "all", "label": "Все районы"}],
        "available_years": option_catalog.get("years") or [{"value": "all", "label": "Все годы"}],
        "available_limits": _build_limit_options(),
        "available_features": _build_access_point_shell_feature_options(request_state["feature_columns"]),
    }
    selected_district_label = _selection_label(filters["available_districts"], selected_district, "Все районы")
    selected_year_label = _selection_label(filters["available_years"], selected_year_value, "Все годы")

    dataset = _load_access_point_dataset(
        request_state["source_tables"],
        district=selected_district,
        selected_year=selected_year,
        metadata_items=metadata_items,
    )
    candidate_features = _build_access_point_candidate_features(dataset["entity_frame"])
    available_feature_names = [str(item.get("name") or "") for item in candidate_features if item.get("name")]
    selected_features, selection_note = _resolve_selected_access_point_features(
        available_feature_names,
        request_state["feature_columns"],
    )
    filters["feature_columns"] = list(selected_features)
    filters["available_features"] = _build_access_point_feature_options(candidate_features, selected_features)
    rows = _build_access_point_rows_from_entity_frame(
        dataset["entity_frame"],
        selected_features=selected_features,
    )
    incomplete_points = _select_incomplete_points(rows)
    summary = _build_summary(
        rows,
        selected_table_label=selected_table_label,
        selected_district_label=selected_district_label,
        selected_year_label=selected_year_label,
        limit=request_state["limit"],
        total_incidents=int(dataset["total_incidents"]),
        incomplete_points=incomplete_points,
    )
    notes = _build_notes(metadata_notes, dataset["notes"], rows, incomplete_points)
    if selection_note:
        notes = [selection_note, *notes]
    if not rows:
        return _ACCESS_POINTS_CACHE.set(
            request_state["cache_key"],
            _sanitize_json_payload({
                **_empty_access_points_data(
                filters=filters,
                summary=summary,
                notes=notes,
                ),
                "charts": _empty_access_points_charts("После расчёта здесь появится график распределения проблемных точек."),
            }),
        )

    top_points = _select_top_points(rows)
    payload = {
        "bootstrap_mode": "resolved",
        "loading": False,
        "has_data": True,
        "title": "Проблемные точки",
        "model_description": (
            "Рейтинг строится по individual points, а не по кластерам территорий: сначала адрес/объект, затем координатная точка,"
            " далее населённый пункт или territory label. Итоговый score комбинирует доступность ПЧ, время прибытия,"
            " подтверждённость водоснабжения, тяжесть последствий, повторяемость пожаров и отдельный слой неполноты данных."
        ),
        "filters": filters,
        "summary": summary,
        "summary_cards": _build_summary_cards(
            rows,
            total_incidents=int(dataset["total_incidents"]),
            incomplete_points=incomplete_points,
        ),
        "charts": {
            "scatter": _build_points_scatter_chart(rows),
        },
        "top_point_label": str(top_points[0]["label"] if top_points else "-"),
        "top_point_explanation": _build_top_point_lead(top_points[0] if top_points else None),
        "points": [dict(row) for row in rows[: request_state["limit"]]],
        "top_points": top_points,
        "score_distribution": _build_score_distribution(rows),
        "reason_breakdown": _build_reason_breakdown(rows),
        "incomplete_points": incomplete_points,
        "typology": _build_typology_rows(rows),
        "uncertainty_notes": _build_uncertainty_notes(rows),
        "notes": notes,
    }
    return _ACCESS_POINTS_CACHE.set(request_state["cache_key"], _sanitize_json_payload(payload))
