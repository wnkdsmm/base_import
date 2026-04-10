from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Dict

try:
    import plotly.graph_objects as go
    from plotly.offline import get_plotlyjs
    from plotly.utils import PlotlyJSONEncoder

    PLOTLY_AVAILABLE = True
except Exception:
    go = None
    get_plotlyjs = None
    PlotlyJSONEncoder = None
    PLOTLY_AVAILABLE = False


DEFAULT_PLOTLY_CONFIG: Dict[str, Any] = {
    "responsive": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": [
        "lasso2d",
        "select2d",
        "autoScale2d",
        "toggleSpikelines",
    ],
}


@lru_cache(maxsize=1)
def get_plotly_bundle() -> str:
    if not PLOTLY_AVAILABLE or get_plotlyjs is None:
        return "window.Plotly = window.Plotly || undefined;"
    try:
        return get_plotlyjs()
    except Exception:
        return "window.Plotly = window.Plotly || undefined;"


def empty_plotly_payload(empty_message: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "data": [],
        "layout": {},
        "config": dict(DEFAULT_PLOTLY_CONFIG),
    }
    if empty_message:
        payload["empty_message"] = empty_message
    return payload


def serialize_plotly_figure(figure: Any) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE or PlotlyJSONEncoder is None:
        return empty_plotly_payload()

    payload = json.loads(json.dumps(figure, cls=PlotlyJSONEncoder))
    if isinstance(payload.get("layout"), dict):
        payload["layout"].pop("template", None)
    payload["config"] = dict(DEFAULT_PLOTLY_CONFIG)
    return payload


def build_empty_plotly_figure_payload(
    empty_message: str = "",
    *,
    height: int = 320,
    annotation_color: str = "#7b6a5a",
    paper_bgcolor: str = "rgba(255,255,255,0)",
    plot_bgcolor: str = "rgba(255,255,255,0)",
    margin: Dict[str, int] | None = None,
    use_plotly: bool = True,
) -> Dict[str, Any]:
    if not use_plotly:
        return empty_plotly_payload()
    if not PLOTLY_AVAILABLE or go is None:
        return empty_plotly_payload(empty_message)

    figure = go.Figure()
    figure.update_layout(
        height=height,
        paper_bgcolor=paper_bgcolor,
        plot_bgcolor=plot_bgcolor,
        margin=margin or {"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": empty_message,
                "x": 0.5,
                "y": 0.5,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16, "color": annotation_color},
            }
        ],
    )
    payload = serialize_plotly_figure(figure)
    if empty_message:
        payload["empty_message"] = empty_message
    return payload
