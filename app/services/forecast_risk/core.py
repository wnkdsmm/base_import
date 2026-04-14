from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Sequence

from app.perf import ensure_sqlalchemy_timing, perf_trace
from config.db import engine

from .constants import MAX_TERRITORIES
from .data import _collect_risk_inputs
from .geo import _build_geo_prediction
from .notes import _build_risk_notes
from .presentation import (
    _build_decision_support_payload_response,
    _build_empty_decision_support_payload,
    _build_feature_cards,
    _build_geo_summary,
    _build_quality_passport,
)
from .profile_resolution import resolve_weight_profile_for_records
from .profiles import DEFAULT_RISK_WEIGHT_MODE, build_weight_profile_snapshot, get_risk_weight_profile
from .reliability import _attach_ranking_reliability, _build_summary_cards, _top_territory_confidence_payload
from .scoring import _build_territory_rows, _top_territory_lead
from .types import (
    FeatureCard,
    GeoPredictionData,
    GeoSummary,
    HistoricalValidationPayload,
    QualityPassport,
    RiskPresentation,
    RiskProfile,
    RiskScore,
)
from .utils import _unique_non_empty
from .validation import build_historical_validation_payload, empty_historical_validation_payload

DECISION_SUPPORT_TITLE = "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№: СЂР°РЅР¶РёСЂРѕРІР°РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёР№"
DECISION_SUPPORT_DESCRIPTION = (
    "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РѕС‚РєСЂС‹РІР°СЋС‚ РїРѕСЃР»Рµ СЃС†РµРЅР°СЂРЅРѕРіРѕ РїСЂРѕРіРЅРѕР·Р°, РєРѕРіРґР° РЅСѓР¶РЅРѕ РїРѕРЅСЏС‚СЊ, РєР°РєРёРµ С‚РµСЂСЂРёС‚РѕСЂРёРё Р±СЂР°С‚СЊ РІ СЂР°Р±РѕС‚Сѓ РїРµСЂРІС‹РјРё. "
    "РћРЅ РЅРµ РїРѕРєР°Р·С‹РІР°РµС‚ РєР°Р»РµРЅРґР°СЂСЊ РїРѕ РґРЅСЏРј Рё РЅРµ РѕС†РµРЅРёРІР°РµС‚ РѕР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ: РµРіРѕ Р·Р°РґР°С‡Р° - СЂР°РЅР¶РёСЂРѕРІР°С‚СЊ С‚РµСЂСЂРёС‚РѕСЂРёРё РїРѕ СЃРѕС‡РµС‚Р°РЅРёСЋ "
    "С‡Р°СЃС‚РѕС‚С‹ РїРѕР¶Р°СЂРѕРІ, С‚СЏР¶РµСЃС‚Рё РїРѕСЃР»РµРґСЃС‚РІРёР№, Р»РѕРіРёСЃС‚РёРєРё РїСЂРёР±С‹С‚РёСЏ Рё РѕР±РµСЃРїРµС‡РµРЅРЅРѕСЃС‚Рё РІРѕРґРѕР№. "
    "РљРѕРјРїРѕРЅРµРЅС‚С‹ РїРѕРєР°Р·Р°РЅС‹ РїСЂРѕР·СЂР°С‡РЅРѕ, С‡С‚РѕР±С‹ СЂРµС€РµРЅРёРµ РјРѕР¶РЅРѕ Р±С‹Р»Рѕ РѕР±СЉСЏСЃРЅРёС‚СЊ Р±РµР· С‡РµСЂРЅРѕРіРѕ СЏС‰РёРєР°."
)


def _feature_coverage_display(feature_cards: Sequence[FeatureCard]) -> str:
    if not feature_cards:
        return "0 РёР· 0"
    available_count = sum(1 for item in feature_cards if item["status"] != "missing")
    return f"{available_count} РёР· {len(feature_cards)}"


def _placeholder_decision_support_notes(preload_notes: Sequence[str]) -> list[str]:
    return _unique_non_empty(
        list(preload_notes[:2])
        + ["РџРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РїРѕ РІС‹Р±СЂР°РЅРЅРѕРјСѓ СЃСЂРµР·Сѓ РґР»СЏ РїСЂРёРѕСЂРёС‚РёР·Р°С†РёРё С‚РµСЂСЂРёС‚РѕСЂРёР№."]
    )


def _validation_windows_count(historical_validation: HistoricalValidationPayload) -> int:
    return int((historical_validation.get("metrics_raw") or {}).get("windows_count") or 0)


def _build_placeholder_decision_support_payload(
    *,
    coverage_display: str,
    quality_passport: QualityPassport,
    feature_cards: Sequence[FeatureCard],
    requested_weight_profile: RiskProfile,
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: GeoPredictionData | None,
) -> RiskPresentation:
    top_confidence = _top_territory_confidence_payload(None, quality_passport)
    return _build_empty_decision_support_payload(
        title=DECISION_SUPPORT_TITLE,
        model_description=DECISION_SUPPORT_DESCRIPTION,
        coverage_display=coverage_display,
        quality_passport=quality_passport,
        top_confidence=top_confidence,
        feature_cards=feature_cards,
        weight_profile=requested_weight_profile,
        historical_validation=empty_historical_validation_payload(
            requested_weight_profile.get("mode_label") or "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°"
        ),
        notes=notes,
        geo_summary=geo_summary,
        geo_prediction=geo_prediction or {},
    )


def _build_ranked_decision_support_payload(
    *,
    coverage_display: str,
    quality_passport: QualityPassport,
    territories: Sequence[RiskScore],
    feature_cards: Sequence[FeatureCard],
    weight_profile: RiskProfile,
    historical_validation: HistoricalValidationPayload,
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: GeoPredictionData | None,
) -> RiskPresentation:
    top_territory = territories[0] if territories else None
    top_confidence = _top_territory_confidence_payload(top_territory, quality_passport)
    return _build_decision_support_payload_response(
        title=DECISION_SUPPORT_TITLE,
        model_description=DECISION_SUPPORT_DESCRIPTION,
        coverage_display=coverage_display,
        quality_passport=quality_passport,
        summary_cards=_build_summary_cards(
            territories,
            weight_profile,
            historical_validation,
            quality_passport,
        ),
        top_territory_label=top_territory["label"] if top_territory else "-",
        top_territory_explanation=_top_territory_lead(top_territory),
        top_confidence=top_confidence,
        territories=territories[:MAX_TERRITORIES],
        feature_cards=feature_cards,
        weight_profile=weight_profile,
        historical_validation=historical_validation,
        notes=notes,
        geo_summary=geo_summary,
        geo_prediction=geo_prediction or {},
    )


def build_decision_support_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    planning_horizon_days: int,
    geo_prediction: Optional[GeoPredictionData] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    selected_year: Optional[int] = None,
    progress_callback: Optional[Callable[[str, str], None]] = None,
    include_geo_prediction: bool = True,
    include_historical_validation: bool = True,
) -> RiskPresentation:
    ensure_sqlalchemy_timing(engine)
    with perf_trace(
        "decision_support",
        source_tables=len(source_tables),
        district=selected_district,
        cause=selected_cause,
        object_category=selected_object_category,
        history_window=history_window,
        planning_horizon_days=planning_horizon_days,
        selected_year=selected_year,
        geo_prediction_reused=geo_prediction is not None,
        geo_prediction_enabled=include_geo_prediction,
        historical_validation_enabled=include_historical_validation,
    ) as perf:
        with perf.span("filter_prep"):
            if progress_callback is not None:
                progress_callback(
                    "decision_support.loading",
                    "РЎРѕР±РёСЂР°РµРј РІС…РѕРґРЅС‹Рµ РґР°РЅРЅС‹Рµ Рё РїСЂРёР·РЅР°РєРё РґР»СЏ Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.",
                )
            metadata_items, filtered_records, preload_notes = _collect_risk_inputs(
                source_tables,
                district=selected_district,
                cause=selected_cause,
                object_category=selected_object_category,
                history_window=history_window,
                selected_year=selected_year,
            )
            feature_cards = _build_feature_cards(metadata_items)
            quality_passport = _build_quality_passport(feature_cards, metadata_items)
            coverage_display = _feature_coverage_display(feature_cards)
            perf.update(
                metadata_tables=len(metadata_items),
                input_rows=len(filtered_records),
                feature_cards=len(feature_cards),
            )

        with perf.span("aggregation"):
            if progress_callback is not None:
                progress_callback(
                    "decision_support.aggregation",
                    "РЎС‚СЂРѕРёРј СЂР°РЅР¶РёСЂРѕРІР°РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёР№, РїР°СЃРїРѕСЂС‚ РєР°С‡РµСЃС‚РІР° Рё РЅСѓР¶РЅС‹Рµ Р°РіСЂРµРіР°С‚С‹.",
                )
            if include_geo_prediction and geo_prediction is None and filtered_records:
                geo_prediction = _build_geo_prediction(filtered_records, planning_horizon_days)
            geo_summary = _build_geo_summary(geo_prediction or {})
            requested_profile = get_risk_weight_profile(weight_mode)
            requested_weight_profile = build_weight_profile_snapshot(requested_profile)

        if not filtered_records:
            with perf.span("payload_render"):
                if progress_callback is not None:
                    progress_callback(
                        "decision_support.render",
                        "РџРѕРґРіРѕС‚Р°РІР»РёРІР°РµРј placeholder-СЂРµР·СѓР»СЊС‚Р°С‚ Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.",
                    )
                notes = _placeholder_decision_support_notes(preload_notes)
                payload = _build_placeholder_decision_support_payload(
                    coverage_display=coverage_display,
                    quality_passport=quality_passport,
                    feature_cards=feature_cards,
                    requested_weight_profile=requested_weight_profile,
                    notes=notes,
                    geo_summary=geo_summary,
                    geo_prediction=geo_prediction,
                )
                perf.update(payload_has_data=False, payload_notes=len(notes), territory_rows=0)
                if progress_callback is not None:
                    progress_callback(
                        "decision_support.completed",
                        "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ Р·Р°РІРµСЂС€РµРЅ РІ СЂРµР¶РёРјРµ placeholder.",
                    )
                return payload

        with perf.span("aggregation"):
            if progress_callback is not None:
                progress_callback(
                    "decision_support.training",
                    (
                        "РћС†РµРЅРёРІР°РµРј СѓСЃС‚РѕР№С‡РёРІРѕСЃС‚СЊ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ Рё РїСЂРѕРІРµСЂСЏРµРј РёСЃС‚РѕСЂРёС‡РµСЃРєСѓСЋ РІР°Р»РёРґР°С†РёСЋ."
                        if include_historical_validation
                        else "РЎРѕР±РёСЂР°РµРј ranking РїРѕ С‚РµСЂСЂРёС‚РѕСЂРёСЏРј Р±РµР· РїРѕР»РЅРѕР№ РёСЃС‚РѕСЂРёС‡РµСЃРєРѕР№ РІР°Р»РёРґР°С†РёРё."
                    ),
                )
            resolved_profile = resolve_weight_profile_for_records(
                filtered_records,
                planning_horizon_days,
                weight_mode=weight_mode,
                enable_calibration=include_historical_validation,
                disabled_summary=(
                    "Р”Р»СЏ РѕР±Р»РµРіС‡РµРЅРЅРѕРіРѕ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРіРѕ snapshot Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєР°СЏ РєР°Р»РёР±СЂРѕРІРєР° РІРµСЃРѕРІ РЅРµ Р·Р°РїСѓСЃРєР°Р»Р°СЃСЊ; РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ Р±Р°Р·РѕРІС‹Р№ РїСЂРѕС„РёР»СЊ."
                    if not include_historical_validation
                    else ""
                ),
            )
            territories = _build_territory_rows(
                filtered_records,
                planning_horizon_days,
                weight_mode=resolved_profile.get("mode") or weight_mode,
                profile_override=resolved_profile,
            )
            historical_validation = empty_historical_validation_payload(
                resolved_profile.get("mode_label") or "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°"
            )
            if include_historical_validation:
                historical_validation = build_historical_validation_payload(
                    filtered_records,
                    planning_horizon_days,
                    weight_mode=resolved_profile.get("mode") or weight_mode,
                    profile_override=resolved_profile,
                )
            territories = _attach_ranking_reliability(territories, quality_passport, historical_validation)
            weight_profile = build_weight_profile_snapshot(resolved_profile)
            perf.update(
                territory_rows=len(territories),
                validation_windows=_validation_windows_count(historical_validation),
            )

        with perf.span("payload_render"):
            if progress_callback is not None:
                progress_callback(
                    "decision_support.render",
                    "РЎРѕР±РёСЂР°РµРј СЂРµРєРѕРјРµРЅРґР°С†РёРё, РєР°СЂС‚С‹ Рё РёС‚РѕРіРѕРІС‹Р№ payload.",
            )
            notes = _build_risk_notes(feature_cards, preload_notes, weight_profile, historical_validation)
            payload = _build_ranked_decision_support_payload(
                coverage_display=coverage_display,
                quality_passport=quality_passport,
                territories=territories,
                feature_cards=feature_cards,
                weight_profile=weight_profile,
                historical_validation=historical_validation,
                notes=notes,
                geo_summary=geo_summary,
                geo_prediction=geo_prediction,
            )
            perf.update(
                payload_has_data=bool(payload["has_data"]),
                payload_notes=len(notes),
            )
            if progress_callback is not None:
                progress_callback("decision_support.completed", "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РіРѕС‚РѕРІ.")
            return payload


def build_risk_forecast_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    forecast_rows: Sequence[dict[str, Any]],  # one-off structure: forecasting rows used only for horizon length
    geo_prediction: Optional[GeoPredictionData] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> RiskPresentation:
    return build_decision_support_payload(
        source_tables=source_tables,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        history_window=history_window,
        planning_horizon_days=max(1, len(forecast_rows) or 14),
        geo_prediction=geo_prediction,
        weight_mode=weight_mode,
    )
