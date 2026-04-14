(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};

    modules.createForecastingUi = function createForecastingUi(options) {
        var createRender = modules.createForecastingRender;
        var createState = modules.createForecastingState;
        if (typeof createRender !== 'function') {
            return null;
        }

        var state = typeof createState === 'function'
            ? createState({ initialData: window.__FIRE_FORECAST_INITIAL__ || null })
            : null;
        var renderUi = createRender(options || {});

        if (!renderUi) {
            return renderUi;
        }

        var originalApply = typeof renderUi.applyForecastData === 'function'
            ? renderUi.applyForecastData.bind(renderUi)
            : null;

        if (originalApply && state && typeof state.setCurrentData === 'function') {
            renderUi.applyForecastData = function applyForecastDataWithState(data) {
                state.setCurrentData(data);
                return originalApply(data);
            };
        }

        if (state && typeof state.getCurrentData === 'function') {
            var originalGet = typeof renderUi.getCurrentForecastData === 'function'
                ? renderUi.getCurrentForecastData.bind(renderUi)
                : null;
            renderUi.getCurrentForecastData = function getCurrentForecastDataWithState() {
                var current = state.getCurrentData();
                if (current != null) {
                    return current;
                }
                return originalGet ? originalGet() : null;
            };
        }

        if (!renderUi.collectForecastFiltersFromForm && state && typeof state.collectSelectedFilters === 'function') {
            renderUi.collectForecastFiltersFromForm = state.collectSelectedFilters;
        }

        return renderUi;
    };
})();

