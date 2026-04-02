from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from .utils import _format_decimal, _format_integer

DEFAULT_RISK_WEIGHT_MODE = "adaptive"

EXPERT_RISK_WEIGHT_PROFILE: Dict[str, Any] = {
    "mode": "expert",
    "mode_label": "Экспертные веса",
    "status_label": "Экспертный профиль",
    "status_tone": "forest",
    "description": (
        "Базовый профиль для сельских территорий. Итоговый риск складывается из четырех компонентов: "
        "частоты пожаров, тяжести последствий, риска долгого прибытия и дефицита водоснабжения."
    ),
    "component_order": [
        "fire_frequency",
        "consequence_severity",
        "long_arrival_risk",
        "water_supply_deficit",
    ],
    "component_weights": {
        "fire_frequency": 0.34,
        "consequence_severity": 0.24,
        "long_arrival_risk": 0.24,
        "water_supply_deficit": 0.18,
    },
    "rural_weight_shift": {
        "fire_frequency": -0.03,
        "consequence_severity": -0.02,
        "long_arrival_risk": 0.03,
        "water_supply_deficit": 0.02,
    },
    "components": {
        "fire_frequency": {
            "label": "Частота пожаров",
            "description": "Повторяемость и близость прошлых пожаров к текущему горизонту планирования.",
            "signals": [
                {"key": "predicted_repeat_rate", "label": "Повторяемость на горизонте", "weight": 0.44},
                {"key": "history_pressure", "label": "Накопленная история", "weight": 0.22},
                {"key": "recency_pressure", "label": "Свежесть случаев", "weight": 0.14},
                {"key": "seasonal_alignment", "label": "Сезонное совпадение", "weight": 0.10},
                {"key": "heating_pressure", "label": "Отопительный контур", "weight": 0.10},
            ],
        },
        "consequence_severity": {
            "label": "Тяжесть последствий",
            "description": "Насколько тяжёлыми были последствия пожаров на территории и каков профиль уязвимости.",
            "signals": [
                {"key": "severe_rate", "label": "Тяжёлые последствия", "weight": 0.34},
                {"key": "casualty_pressure", "label": "Пострадавшие и погибшие", "weight": 0.24},
                {"key": "damage_pressure", "label": "Ущерб и уничтожение", "weight": 0.20},
                {"key": "risk_category_factor", "label": "Категория риска", "weight": 0.14},
                {"key": "heating_pressure", "label": "Отопительный контур", "weight": 0.08},
            ],
        },
        "long_arrival_risk": {
            "label": "Риск долгого прибытия",
            "description": "Логистический риск: фактическое прибытие, explainable travel-time, покрытие ПЧ и сервисная зона территории.",
            "signals": [
                {"key": "long_arrival_rate", "label": "Доля долгих прибытий", "weight": 0.24},
                {"key": "avg_response_pressure", "label": "Среднее время прибытия", "weight": 0.18},
                {"key": "travel_time_pressure", "label": "Travel-time доезда", "weight": 0.22},
                {"key": "service_coverage_gap", "label": "Дефицит покрытия ПЧ", "weight": 0.20},
                {"key": "service_zone_pressure", "label": "Сервисная зона", "weight": 0.10},
                {"key": "distance_pressure", "label": "Удалённость до ПЧ", "weight": 0.06},
            ],
        },
        "water_supply_deficit": {
            "label": "Дефицит водоснабжения",
            "description": "Риск недостатка наружного водоснабжения и зависимости от подвоза воды на удалённой территории.",
            "signals": [
                {"key": "water_gap_rate", "label": "Подтвержденный дефицит воды", "weight": 0.62},
                {"key": "tanker_dependency", "label": "Зависимость от подвоза", "weight": 0.18},
                {"key": "rural_context", "label": "Сельский контекст", "weight": 0.12},
                {"key": "damage_pressure", "label": "История ущерба", "weight": 0.08},
            ],
        },
    },
    "thresholds": {
        "risk_class": {"high": 67.0, "medium": 43.0},
        "priority": {"immediate": 70.0, "targeted": 45.0},
        "component": {"high": 65.0, "medium": 40.0},
    },
    "defaults": {
        "water_gap_unknown": 0.38,
        "distance_km_baseline": 12.0,
        "distance_pressure_unknown": 0.30,
        "response_pressure_unknown": 0.42,
    },
    "notes": [
        "Для сельских территорий вес логистики и водоснабжения автоматически повышается.",
        "Веса компонентов и сигналов вынесены в отдельную структуру и могут меняться без переписывания формулы.",
        "Итоговый балл интерпретируется как управленческий приоритет территории, а не как прямой прогноз числа пожаров.",
    ],
    "calibration": {
        "ready": False,
        "targets": [
            "top1_hit_rate",
            "top3_capture_rate",
            "precision_at_3",
            "ndcg_at_3",
        ],
        "notes": [
            "Экспертный профиль остается резервным режимом, если истории недостаточно для устойчивой калибровки.",
        ],
    },
}

ADAPTIVE_RISK_WEIGHT_PROFILE: Dict[str, Any] = deepcopy(EXPERT_RISK_WEIGHT_PROFILE)
ADAPTIVE_RISK_WEIGHT_PROFILE.update(
    {
        "mode": "adaptive",
        "mode_label": "Адаптивные веса",
        "status_label": "Ожидает калибровку",
        "status_tone": "sand",
        "description": (
            "Сервис старается подобрать веса компонентов по историческим окнам ranking-качества, а если данных мало, "
            "автоматически возвращается к экспертному профилю без потери объяснимости."
        ),
        "notes": [
            "Подстраиваются только веса четырех понятных компонентов, а сами сигналы внутри компонентов остаются прозрачными и доступны для расшифровки.",
            "Если историческая проверка не дает устойчивого выигрыша, сервис удерживает экспертные веса как fallback.",
        ],
        "calibration": {
            "ready": True,
            "targets": [
                "top1_hit_rate",
                "top3_capture_rate",
                "precision_at_3",
                "ndcg_at_3",
            ],
            "notes": [
                "Калибровка идет по историческим окнам без использования будущих наблюдений внутри каждого окна.",
                "Подбор оптимизирует ranking-метрики, а не скрытую черную коробку по территориям.",
            ],
        },
    }
)

CALIBRATABLE_RISK_WEIGHT_PROFILE: Dict[str, Any] = deepcopy(ADAPTIVE_RISK_WEIGHT_PROFILE)
CALIBRATABLE_RISK_WEIGHT_PROFILE.update(
    {
        "mode": "calibratable",
        "mode_label": "Шаблон для настройки",
        "status_label": "Ручная настройка",
        "status_tone": "sky",
        "description": (
            "Режим с той же прозрачной структурой компонентов, предназначенный для ручной настройки или экспериментального "
            "подбора весов по историческим окнам."
        ),
        "notes": [
            "Использует ту же компонентную формулу, что и экспертный профиль.",
            "Подходит для ручного сравнения альтернативных наборов весов без смены интерфейса.",
        ],
    }
)

RISK_WEIGHT_PROFILES: Dict[str, Dict[str, Any]] = {
    DEFAULT_RISK_WEIGHT_MODE: ADAPTIVE_RISK_WEIGHT_PROFILE,
    "expert": EXPERT_RISK_WEIGHT_PROFILE,
    "calibratable": CALIBRATABLE_RISK_WEIGHT_PROFILE,
}



def get_risk_weight_profile(mode: str = DEFAULT_RISK_WEIGHT_MODE) -> Dict[str, Any]:
    resolved_mode = mode if mode in RISK_WEIGHT_PROFILES else DEFAULT_RISK_WEIGHT_MODE
    return deepcopy(RISK_WEIGHT_PROFILES[resolved_mode])



def resolve_component_weights(profile: Dict[str, Any], is_rural: bool) -> List[Dict[str, Any]]:
    base_weights = {key: float(value) for key, value in (profile.get("component_weights") or {}).items()}
    adjusted_weights = dict(base_weights)
    if is_rural:
        for key, shift in (profile.get("rural_weight_shift") or {}).items():
            adjusted_weights[key] = max(0.0, adjusted_weights.get(key, 0.0) + float(shift))
    total_weight = sum(adjusted_weights.values()) or 1.0

    rows: List[Dict[str, Any]] = []
    for key in profile.get("component_order", []):
        spec = (profile.get("components") or {}).get(key, {})
        weight = adjusted_weights.get(key, 0.0) / total_weight
        base_weight = base_weights.get(key, 0.0)
        rows.append(
            {
                "key": key,
                "label": spec.get("label") or key,
                "description": spec.get("description") or "",
                "weight": weight,
                "weight_display": _format_weight(weight),
                "base_weight": base_weight,
                "base_weight_display": _format_weight(base_weight),
                "rural_shift": weight - base_weight,
                "rural_shift_display": _format_shift(weight - base_weight),
            }
        )
    return rows



def build_weight_profile_snapshot(profile: Dict[str, Any]) -> Dict[str, Any]:
    base_components = resolve_component_weights(profile, is_rural=False)
    rural_components = {item["key"]: item for item in resolve_component_weights(profile, is_rural=True)}
    expert_component_weights = {
        key: float(value)
        for key, value in (profile.get("expert_component_weights") or profile.get("component_weights") or {}).items()
    }

    components: List[Dict[str, Any]] = []
    for item in base_components:
        rural_item = rural_components.get(item["key"], item)
        expert_weight = float(expert_component_weights.get(item["key"], item["weight"]))
        calibration_shift = item["weight"] - expert_weight
        components.append(
            {
                **item,
                "expert_weight": expert_weight,
                "expert_weight_display": _format_weight(expert_weight),
                "current_weight_display": item["weight_display"],
                "calibration_shift": calibration_shift,
                "calibration_shift_display": _format_shift(calibration_shift),
                "rural_weight": rural_item.get("weight", item["weight"]),
                "rural_weight_display": rural_item.get("weight_display", item["weight_display"]),
            }
        )

    available_modes: List[Dict[str, str]] = []
    for key, value in RISK_WEIGHT_PROFILES.items():
        available_modes.append(
            {
                "mode": key,
                "label": value.get("mode_label") or key,
                "status_label": "Активен" if key == profile.get("mode") else value.get("status_label") or "Доступен",
                "status_tone": value.get("status_tone") or ("forest" if key == profile.get("mode") else "sky"),
                "description": value.get("description") or "",
            }
        )

    calibration = profile.get("calibration") or {}
    selected_metrics = calibration.get("selected_metrics") or {}
    expert_metrics = calibration.get("expert_metrics") or {}
    comparison = calibration.get("comparison") or {}
    calibration_notes = list(calibration.get("notes") or [])
    summary = str(calibration.get("summary") or "").strip()
    if summary and summary not in calibration_notes:
        calibration_notes.insert(0, summary)
    comparison_summary = str(comparison.get("summary") or "").strip()
    if comparison_summary and comparison_summary not in calibration_notes:
        calibration_notes.insert(1 if calibration_notes else 0, comparison_summary)

    metric_cards: List[Dict[str, str]] = []
    k_value = int(selected_metrics.get("k_value") or 3)
    if selected_metrics:
        metric_cards.append(
            {
                "label": "Top-1 hit",
                "value": _format_probability(float(selected_metrics.get("top1_hit_rate") or 0.0)),
                "meta": "Доля окон, где территория-лидер действительно горела в следующем интервале.",
            }
        )
        metric_cards.append(
            {
                "label": f"Top-{k_value} capture",
                "value": _format_probability(float(selected_metrics.get("topk_capture_rate") or 0.0)),
                "meta": "Доля будущих пожаров, попавших в верхние территории рейтинга.",
            }
        )
        metric_cards.append(
            {
                "label": f"Precision@{k_value}",
                "value": _format_probability(float(selected_metrics.get("precision_at_k") or 0.0)),
                "meta": (
                    f"Экспертный профиль: {_format_probability(float(expert_metrics.get('precision_at_k') or 0.0))}; Δ { _format_shift_probability(float(comparison.get('precision_at_k_delta') or 0.0)) }"
                    if expert_metrics
                    else "Сколько территорий в top-k действительно подтвердились пожаром."
                ),
            }
        )
        metric_cards.append(
            {
                "label": f"NDCG@{k_value}",
                "value": _format_decimal(float(selected_metrics.get("ndcg_at_k") or 0.0)),
                "meta": (
                    f"Экспертный профиль: {_format_decimal(float(expert_metrics.get('ndcg_at_k') or 0.0))}; Δ { _format_signed_decimal(float(comparison.get('ndcg_at_k_delta') or 0.0)) }"
                    if expert_metrics
                    else "Сравнение с экспертным профилем недоступно."
                ),
            }
        )

    return {
        "mode": profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
        "mode_label": profile.get("mode_label") or "Адаптивные веса",
        "status_label": profile.get("status_label") or "Активный профиль",
        "status_tone": profile.get("status_tone") or "forest",
        "description": profile.get("description") or "",
        "components": components,
        "available_modes": available_modes,
        "notes": list(profile.get("notes") or []),
        "calibration_ready": bool(calibration.get("ready")),
        "calibration_targets": list(calibration.get("targets") or []),
        "calibration_notes": calibration_notes,
        "calibration_summary": summary,
        "calibration_windows_display": _format_integer(calibration.get("windows_used") or 0),
        "calibration_candidate_count_display": _format_integer(calibration.get("candidate_count") or 0),
        "uses_fallback": bool(calibration.get("used_fallback")),
        "calibration_comparison": comparison,
        "metric_cards": metric_cards,
    }



def _format_weight(value: float) -> str:
    percent = round(float(value) * 100.0)
    return f"{int(percent)}%"



def _format_shift(value: float) -> str:
    points = round(float(value) * 100.0)
    sign = "+" if points > 0 else ""
    return f"{sign}{int(points)} п.п."



def _format_probability(value: float) -> str:
    rounded = round(float(value) * 100.0, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"{int(round(rounded))}%"
    return f"{str(rounded).replace('.', ',')}%"



def _format_shift_probability(value: float) -> str:
    numeric = round(float(value) * 100.0, 1)
    sign = "+" if numeric > 0 else ""
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{sign}{int(round(numeric))} п.п."
    return f"{sign}{str(numeric).replace('.', ',')} п.п."


def _format_signed_decimal(value: float) -> str:
    numeric = round(float(value), 3)
    sign = "+" if numeric > 0 else ""
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{sign}{int(round(numeric))}"
    return sign + str(numeric).replace('.', ',')

