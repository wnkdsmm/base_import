(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};

    modules.initForecastingPage = function initForecastingPage(options) {
        var ui = options && options.ui ? options.ui : null;
        var api = options && options.api ? options.api : null;

        if (!ui || !api) {
            return;
        }

        function syncBriefLink() {
            var filters = ui.collectForecastFiltersFromForm();
            ui.updateForecastBriefExport(filters);
            ui.updateForecastScreenLinks(filters);
        }

        function bindForecastForm(form) {
            if (!form) {
                return;
            }

            form.addEventListener('submit', function (event) {
                event.preventDefault();
                api.fetchForecastData();
            });

            Array.prototype.forEach.call(form.querySelectorAll('select, input'), function (field) {
                field.addEventListener('change', syncBriefLink);
                if (field.tagName === 'INPUT') {
                    field.addEventListener('input', syncBriefLink);
                }
            });
        }

        function bindRetryButton() {
            var retryButton = shared.byId('forecastRetryButton');
            if (!retryButton) {
                return;
            }
            retryButton.addEventListener('click', function () {
                api.fetchForecastData();
            });
        }

        function bootstrapForecasting(form) {
            var initialData = window.__FIRE_FORECAST_INITIAL__ || null;

            syncBriefLink();
            if (initialData) {
                ui.syncForecastStageVisibility(initialData);
                if (initialData.bootstrap_mode !== 'deferred') {
                    ui.applyForecastData(initialData);
                }
            }

            if (!initialData || initialData.bootstrap_mode === 'deferred') {
                api.fetchForecastData();
                return;
            }

            if (initialData.decision_support_pending && form) {
                api.startDecisionSupportFromQuery(new URLSearchParams(new FormData(form)).toString());
            }
        }

        window.fetchForecastData = function fetchForecastDataGlobal() {
            return api.fetchForecastData();
        };
        window.downloadAnalyticalBrief = function downloadAnalyticalBriefGlobal() {
            return ui.downloadAnalyticalBrief();
        };

        var form = shared.byId('forecastForm');
        ui.applyProgressBars(document);
        bindForecastForm(form);
        bindRetryButton();
        bootstrapForecasting(form);
    };
})();

