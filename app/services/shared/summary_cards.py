from __future__ import annotations

from typing import TypedDict


class SummaryCardInput(TypedDict, total=False):
    label: object
    value: object
    meta: object
    tone: object
    include: bool


def build_summary_cards(items: list[SummaryCardInput]) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    for item in items:
        if item.get("include") is False:
            continue
        card: dict[str, str] = {
            "label": str(item.get("label") or ""),
            "value": str(item.get("value") or ""),
            "meta": str(item.get("meta") or ""),
        }
        tone_value = item.get("tone")
        if tone_value is not None and str(tone_value).strip():
            card["tone"] = str(tone_value)
        cards.append(card)
    return cards

