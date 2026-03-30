from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Sequence

from app.perf import ensure_sqlalchemy_timing, perf_trace
from config.db import engine

from .constants import MAX_TERRITORIES
from .data import _collect_risk_inputs
from .presentation import _build_feature_cards, _build_geo_summary, _build_quality_passport, _build_risk_notes
from .profiles import DEFAULT_RISK_WEIGHT_MODE, build_weight_profile_snapshot, get_risk_weight_profile
from .scoring import _build_territory_rows, _top_territory_lead
from .utils import _clamp, _format_integer, _format_number, _format_probability, _unique_non_empty
from .validation import (
    build_historical_validation_payload,
    empty_historical_validation_payload,
    resolve_weight_profile_for_records,
)

DECISION_SUPPORT_TITLE = "Блок поддержки решений: ранжирование территорий"
DECISION_SUPPORT_DESCRIPTION = (
    "Это прозрачный блок поддержки решений по территориям. Итоговый риск раскладывается на четыре объяснимых компонента: "
    "частоту пожаров, тяжесть последствий, риск долгого прибытия и дефицит водоснабжения. "
    "Логистический компонент раскрывается через фактическое прибытие, explainable travel-time, покрытие ПЧ и сервисную зону, "
    "а веса компонентов при достаточной истории калибруются по rolling-origin окнам без превращения модели в black box."
)


def build_decision_support_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    planning_horizon_days: int,
    geo_prediction: Optional[Dict[str, Any]] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    selected_year: Optional[int] = None,
    progress_callback: Optional[Callable[[str, str], None]] = None,
) -> Dict[str, Any]:
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
    ) as perf:
        with perf.span("filter_prep"):
            if progress_callback is not None:
                progress_callback("decision_support.loading", "Собираем входные данные и признаки для блока поддержки решений.")
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
            coverage_display = (
                f"{sum(1 for item in feature_cards if item['status'] != 'missing')} из {len(feature_cards)}"
                if feature_cards
                else "0 из 0"
            )
            perf.update(
                metadata_tables=len(metadata_items),
                input_rows=len(filtered_records),
                feature_cards=len(feature_cards),
            )

        with perf.span("aggregation"):
            if progress_callback is not None:
                progress_callback("decision_support.aggregation", "Строим ranking территорий, паспорт качества и геосводку.")
            if geo_prediction is None and filtered_records:
                from app.services.forecasting.geo import _build_geo_prediction

                geo_prediction = _build_geo_prediction(filtered_records, planning_horizon_days)
            geo_summary = _build_geo_summary(geo_prediction or {})
            requested_profile = get_risk_weight_profile(weight_mode)
            requested_weight_profile = build_weight_profile_snapshot(requested_profile)

        if not filtered_records:
            with perf.span("payload_render"):
                if progress_callback is not None:
                    progress_callback("decision_support.render", "Подготавливаем placeholder-результат блока поддержки решений.")
                notes = _unique_non_empty(
                    list(preload_notes[:2])
                    + ["Пока недостаточно данных по выбранному срезу для приоритизации территорий."]
                )
                top_confidence = _top_territory_confidence_payload(None, quality_passport)
                payload = {
                    "has_data": False,
                    "title": DECISION_SUPPORT_TITLE,
                    "model_description": DECISION_SUPPORT_DESCRIPTION,
                    "coverage_display": coverage_display,
                    "quality_passport": quality_passport,
                    "summary_cards": [],
                    "top_territory_label": "-",
                    "top_territory_explanation": "Недостаточно данных для ранжирования территорий.",
                    "top_territory_confidence_label": top_confidence["label"],
                    "top_territory_confidence_score_display": top_confidence["score_display"],
                    "top_territory_confidence_tone": top_confidence["tone"],
                    "top_territory_confidence_note": top_confidence["note"],
                    "territories": [],
                    "feature_cards": feature_cards,
                    "weight_profile": requested_weight_profile,
                    "historical_validation": empty_historical_validation_payload(
                        requested_weight_profile.get("mode_label") or "Адаптивные веса"
                    ),
                    "notes": notes,
                    "geo_summary": geo_summary,
                    "geo_prediction": geo_prediction or {},
                }
                perf.update(payload_has_data=False, payload_notes=len(notes), territory_rows=0)
                if progress_callback is not None:
                    progress_callback("decision_support.completed", "Блок поддержки решений завершен в режиме placeholder.")
                return payload

        with perf.span("aggregation"):
            if progress_callback is not None:
                progress_callback("decision_support.training", "Оцениваем устойчивость ranking и проверяем историческую валидацию.")
            resolved_profile = resolve_weight_profile_for_records(
                filtered_records,
                planning_horizon_days,
                weight_mode=weight_mode,
            )
            territories = _build_territory_rows(
                filtered_records,
                planning_horizon_days,
                weight_mode=resolved_profile.get("mode") or weight_mode,
                profile_override=resolved_profile,
            )
            historical_validation = build_historical_validation_payload(
                filtered_records,
                planning_horizon_days,
                weight_mode=resolved_profile.get("mode") or weight_mode,
                profile_override=resolved_profile,
            )
            territories = _attach_ranking_reliability(territories, quality_passport, historical_validation)
            weight_profile = build_weight_profile_snapshot(resolved_profile)
            top_territory = territories[0] if territories else None
            top_confidence = _top_territory_confidence_payload(top_territory, quality_passport)
            perf.update(
                territory_rows=len(territories),
                validation_windows=((historical_validation.get("metrics_raw") or {}).get("windows_count") or 0),
            )

        with perf.span("payload_render"):
            if progress_callback is not None:
                progress_callback("decision_support.render", "Собираем рекомендации, карты и итоговый payload.")
            notes = _build_risk_notes(feature_cards, preload_notes, weight_profile, historical_validation)
            payload = {
                "has_data": bool(territories),
                "title": DECISION_SUPPORT_TITLE,
                "model_description": DECISION_SUPPORT_DESCRIPTION,
                "coverage_display": coverage_display,
                "quality_passport": quality_passport,
                "summary_cards": _build_summary_cards(territories, weight_profile, historical_validation, quality_passport),
                "top_territory_label": top_territory["label"] if top_territory else "-",
                "top_territory_explanation": _top_territory_lead(top_territory),
                "top_territory_confidence_label": top_confidence["label"],
                "top_territory_confidence_score_display": top_confidence["score_display"],
                "top_territory_confidence_tone": top_confidence["tone"],
                "top_territory_confidence_note": top_confidence["note"],
                "territories": territories[:MAX_TERRITORIES],
                "feature_cards": feature_cards,
                "weight_profile": weight_profile,
                "historical_validation": historical_validation,
                "notes": notes,
                "geo_summary": geo_summary,
                "geo_prediction": geo_prediction or {},
            }
            perf.update(
                payload_has_data=bool(payload["has_data"]),
                payload_notes=len(notes),
            )
            if progress_callback is not None:
                progress_callback("decision_support.completed", "Блок поддержки решений готов.")
            return payload


def build_risk_forecast_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    forecast_rows: Sequence[Dict[str, Any]],
    geo_prediction: Optional[Dict[str, Any]] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> Dict[str, Any]:
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



def _attach_ranking_reliability(
    territories: Sequence[Dict[str, Any]],
    quality_passport: Dict[str, Any],
    historical_validation: Dict[str, Any],
) -> list[Dict[str, Any]]:
    if not territories:
        return []

    annotated = [dict(item) for item in territories]
    metrics = historical_validation.get("metrics_raw") or {}
    windows_count = int(metrics.get("windows_count") or 0)
    k_value = int(metrics.get("k_value") or 3)
    validation_ready = bool(historical_validation.get("has_metrics")) and windows_count >= 3
    passport_score = float(quality_passport.get("confidence_score") or 0.0) / 100.0
    objective_score = float(metrics.get("objective_score") or 0.0)
    topk_capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision_at_k = float(metrics.get("precision_at_k") or 0.0)
    ndcg_at_k = float(metrics.get("ndcg_at_k") or 0.0)

    for index, territory in enumerate(annotated):
        history_support = min(1.0, float(territory.get("history_count") or 0.0) / 8.0)
        if index == 0:
            margin_support = min(1.0, float(territory.get("ranking_gap_to_next") or 0.0) / 8.0)
        else:
            margin_support = 1.0 - min(1.0, float(territory.get("ranking_gap_to_top") or 0.0) / 12.0)
        local_support = _clamp(0.58 * margin_support + 0.42 * history_support, 0.15, 1.0)

        if validation_ready:
            confidence_norm = _clamp(0.42 * passport_score + 0.38 * objective_score + 0.20 * local_support, 0.18, 0.96)
        else:
            confidence_norm = _clamp(0.67 * passport_score + 0.33 * local_support, 0.16, 0.88)

        if index == 0:
            confidence_norm = _clamp(confidence_norm + 0.03, 0.18, 0.96)
        elif index >= 3:
            confidence_norm = _clamp(confidence_norm - 0.04, 0.16, 0.92)

        confidence_score = int(round(confidence_norm * 100.0))
        label, tone, prefix = _ranking_confidence_state(confidence_score)

        if validation_ready:
            history_clause = (
                f"rolling-origin проверка на {_format_integer(windows_count)} окнах даёт Top-{k_value} capture "
                f"{_format_probability(topk_capture)}, Precision@{k_value} {_format_probability(precision_at_k)} "
                f"и NDCG@{k_value} {_format_number(ndcg_at_k)}"
            )
        else:
            history_clause = (
                f"полной rolling-origin проверки пока нет, поэтому опора идёт на паспорт данных "
                f"{quality_passport.get('confidence_score_display') or '0 / 100'}"
            )

        margin_clause = (
            f"отрыв от следующей территории {territory.get('ranking_gap_to_next_display') or '0 баллов'}"
            if index == 0
            else f"отставание от лидера {territory.get('ranking_gap_to_top_display') or '0 баллов'}"
        )
        component_clause = territory.get("ranking_component_lead") or territory.get("drivers_display") or "компоненты риска территории"
        territory.update(
            {
                "ranking_confidence_score": confidence_score,
                "ranking_confidence_display": f"{confidence_score} / 100",
                "ranking_confidence_label": label,
                "ranking_confidence_tone": tone,
                "ranking_confidence_note": f"{prefix}: {history_clause}; {margin_clause}; основной вклад дают {component_clause}.",
            }
        )

    return annotated



def _top_territory_confidence_payload(
    top_territory: Optional[Dict[str, Any]],
    quality_passport: Dict[str, Any],
) -> Dict[str, str]:
    if top_territory:
        return {
            "label": top_territory.get("ranking_confidence_label") or "Умеренная",
            "score_display": top_territory.get("ranking_confidence_display") or quality_passport.get("confidence_score_display") or "0 / 100",
            "tone": top_territory.get("ranking_confidence_tone") or quality_passport.get("confidence_tone") or "fire",
            "note": top_territory.get("ranking_confidence_note") or quality_passport.get("validation_summary") or "Пояснение появится после расчёта.",
        }

    return {
        "label": quality_passport.get("confidence_label") or "Ограниченная",
        "score_display": quality_passport.get("confidence_score_display") or "0 / 100",
        "tone": quality_passport.get("confidence_tone") or "fire",
        "note": quality_passport.get("validation_summary") or "Пояснение появится после расчёта.",
    }



def _ranking_confidence_state(score: int) -> tuple[str, str, str]:
    if score >= 82:
        return "Высокая", "forest", "Вывод подтверждается уверенно"
    if score >= 64:
        return "Рабочая", "sky", "Вывод подтверждается на рабочем уровне"
    if score >= 46:
        return "Умеренная", "sand", "Вывод полезен для приоритизации, но требует локальной проверки"
    return "Ограниченная", "fire", "Вывод стоит использовать как сигнал к дополнительной проверке"



def _build_summary_cards(
    territories: Sequence[Dict[str, Any]],
    weight_profile: Dict[str, Any],
    historical_validation: Dict[str, Any],
    quality_passport: Dict[str, Any],
) -> list[Dict[str, str]]:
    if not territories:
        return []

    lead = territories[0]
    cards = [
        {
            "label": "Территория первого приоритета",
            "value": lead.get("label") or "-",
            "meta": lead.get("ranking_reason") or lead.get("drivers_display") or "После расчёта здесь появится объяснение лидерства.",
            "tone": lead.get("priority_tone") or "sand",
        },
        {
            "label": "Надёжность вывода",
            "value": lead.get("ranking_confidence_label") or "Умеренная",
            "meta": lead.get("ranking_confidence_note") or "После расчёта здесь появится оценка надёжности ranking-вывода.",
            "tone": lead.get("ranking_confidence_tone") or "fire",
        },
        {
            "label": "Профиль весов",
            "value": weight_profile.get("status_label") or "Активный профиль",
            "meta": weight_profile.get("mode_label") or "Адаптивные веса",
            "tone": weight_profile.get("status_tone") or "forest",
        },
        {
            "label": "Качество данных",
            "value": quality_passport.get("confidence_label") or "Ограниченная",
            "meta": quality_passport.get("validation_summary") or "Паспорт качества появится после расчёта.",
            "tone": quality_passport.get("confidence_tone") or "fire",
        },
    ]

    metrics = historical_validation.get("metrics_raw") or {}
    if historical_validation.get("has_metrics"):
        k_value = int(metrics.get("k_value") or 3)
        cards.extend(
            [
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(float(metrics.get("top1_hit_rate") or 0.0)),
                    "meta": "Как часто первая территория действительно горела в следующем окне",
                    "tone": "sky",
                },
                {
                    "label": f"Top-{k_value} capture",
                    "value": _format_probability(float(metrics.get("topk_capture_rate") or 0.0)),
                    "meta": "Какая доля будущих пожаров попадала в верхнюю часть рейтинга",
                    "tone": "forest",
                },
                {
                    "label": f"Precision@{k_value}",
                    "value": _format_probability(float(metrics.get("precision_at_k") or 0.0)),
                    "meta": "Какая доля территорий в верхней части рейтинга действительно подтверждалась пожаром",
                    "tone": "sky",
                },
                {
                    "label": f"NDCG@{k_value}",
                    "value": _format_number(float(metrics.get("ndcg_at_k") or 0.0)),
                    "meta": "Насколько порядок территорий совпадал с реальной концентрацией пожаров",
                    "tone": "sand",
                },
            ]
        )
    else:
        cards.append(
            {
                "label": "Историческая проверка",
                "value": historical_validation.get("status_label") or "Пока без проверки",
                "meta": historical_validation.get("summary") or "Метрики появятся после накопления истории.",
                "tone": historical_validation.get("status_tone") or "sand",
            }
        )

    return cards
