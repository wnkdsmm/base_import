from __future__ import annotations

from importlib import import_module
from typing import Callable

_INVALIDATORS: tuple[tuple[str, str, str, str], ...] = (
    (
        "db_metadata",
        "app.db_metadata",
        "invalidate_db_metadata_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043c\u0435\u0442\u0430\u0434\u0430\u043d\u043d\u044b\u0445 "
        "\u0411\u0414: {exc}",
    ),
    (
        "dashboard",
        "app.dashboard.cache",
        "_invalidate_dashboard_caches",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043f\u0430\u043d\u0435\u043b\u0438: {exc}",
    ),
    (
        "ml_model",
        "app.services.ml_model.core",
        "clear_ml_model_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 ML-\u0431\u043b\u043e\u043a\u0430: {exc}",
    ),
    (
        "forecasting",
        "app.services.forecasting.core",
        "clear_forecasting_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0438\u0440\u043e\u0432"
        "\u0430\u043d\u0438\u044f: {exc}",
    ),
    (
        "clustering",
        "app.services.clustering.core",
        "clear_clustering_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430"
        "\u0446\u0438\u0438: {exc}",
    ),
    (
        "access_points",
        "app.services.access_points.core",
        "clear_access_points_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043f\u0440\u043e\u0431\u043b\u0435\u043c\u043d\u044b\u0445 "
        "\u0442\u043e\u0447\u0435\u043a: {exc}",
    ),
    (
        "fire_map",
        "app.services.fire_map_service",
        "clear_fire_map_cache",
        "\u041f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0435\u043d\u0438\u0435 "
        "\u043f\u0440\u0438 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0438 "
        "\u043a\u044d\u0448\u0430 \u043a\u0430\u0440\u0442\u044b \u043f\u043e\u0436\u0430\u0440"
        "\u043e\u0432: {exc}",
    ),
)


def _resolve_invalidator(module_name: str, attr_name: str) -> Callable[[], None]:
    module = import_module(module_name)
    return getattr(module, attr_name)


def invalidate_runtime_caches(
    on_warning: Callable[[str], None] | None = None,
    *,
    include_metadata: bool = True,
) -> None:
    def warn(message: str) -> None:
        if on_warning is not None:
            on_warning(message)

    for name, module_name, attr_name, error_template in _INVALIDATORS:
        if not include_metadata and name == "db_metadata":
            continue
        try:
            invalidator = _resolve_invalidator(module_name, attr_name)
            invalidator()
        except Exception as exc:
            warn(error_template.format(exc=exc))


def invalidate_service_caches(on_warning: Callable[[str], None] | None = None) -> None:
    invalidate_runtime_caches(on_warning=on_warning, include_metadata=False)
