# Core/App Architecture Review (2026-04-15)

## 1) Duplicate logic between `core/processing` and `app/services/shared`

Detected analogous helpers (same intent, different namespace context):

- `core/processing/steps/fire_map_loader.py::_normalize_column_name`
  and `app/services/shared/data_base.py::DataLoader.normalize_value`
  - both normalize incoming values via trim/lower/empty handling.
- `core/processing/steps/fire_map_loader.py::_quote_identifier`
  and `app/services/shared/data_utils.py::_quote_identifier`
  - both quote SQL identifiers for safe dynamic SQL generation.
- `core/processing/steps/column_filter_text.py::_normalize_column_text`
  and `app/services/shared/data_utils.py::_normalize_match_text`
  - both normalize free-form text/tokens for matching.

Note: full in-place merge of these helpers is intentionally deferred because `core/` is still consumed by both legacy and web paths, and cross-importing `app.services.*` from `core` would invert layering.

## 2) Is `core/` removable now?

No. `core/` is still required at runtime by web services and tests:

- `app/services/pipeline_service.py` imports `core.processing.steps.*`
- `app/services/fire_map_service.py` imports `core.processing.steps.create_fire_map`
- `app/services/table_workflows.py` imports `core.processing.steps.keep_important_columns`
- tests import `core.processing.*` and `core.mapping.*`

Therefore:
- `core/` namespace is kept
- root `main.py` is kept (legacy desktop entrypoint)

## 3) Constants consolidation

Moved canonical business thresholds to `config/constants.py`:

- `LONG_RESPONSE_THRESHOLD_MINUTES`
- `MIN_TEMPERATURE_NON_NULL_DAYS`
- `MIN_TEMPERATURE_COVERAGE`
- `GEO_LOOKBACK_DAYS`
- `MAX_GEO_CHART_POINTS`
- `MAX_GEO_HOTSPOTS`
- `SERVICE_TIME_TARGET_MINUTES`
- `SERVICE_DISTANCE_TARGET_KM`
- `CORE_SERVICE_TIME_MINUTES`

Compatibility layer kept:

- `app/domain/predictive_settings.py` now re-exports from `config.constants`.

Service imports updated to use canonical constants directly.

## 4) `app/domain` evaluation

`app/domain` is not only TypedDict/Enum. It includes real domain metadata and helper logic:

- `column_matching.py` contains rule-building functions and catalog assembly.
- other modules host domain registries and labels used across services.

Decision:
- keep `app/domain` as domain metadata/registry layer
- avoid moving it into `config/` (which should stay framework-agnostic and avoid domain-rule construction logic)

## 5) Removed endpoints

- `POST /clear_logs` (from `app/routes/api_ops.py`)
  - removed because there are no frontend callers in `app/static/js` or templates, and no infra/config usage was found.
  - related service helper `app/services/ops_service.py::clear_logs_payload` removed as dead code.

- `GET /health` (from `app/routes/api_ops.py`)
  - removed because there are no frontend callers and no Docker/K8s/external healthcheck references in project config.
  - related service helper `app/services/ops_service.py::build_health_payload` removed as dead code.
