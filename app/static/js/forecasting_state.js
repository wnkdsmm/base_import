(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};
    var byId = shared.byId;
    var factory = window.FireStateFactory || {};
    var createStateManager = factory.createStateManager;

    modules.createForecastingState = function createForecastingState(options) {
        var initialData = options && options.initialData ? options.initialData : null;
        var manager = typeof createStateManager === 'function'
            ? createStateManager({ currentData: initialData })
            : null;
        var currentData = initialData;

        function setCurrentData(data) {
            if (!manager) {
                currentData = data || null;
                return currentData;
            }
            return manager.set('currentData', data || null);
        }

        function getCurrentData() {
            return manager ? manager.get('currentData') : currentData;
        }

        function collectSelectedFilters() {
            return {
                table_name: byId('forecastTableFilter') ? byId('forecastTableFilter').value : '',
                district: byId('forecastDistrictFilter') ? byId('forecastDistrictFilter').value : 'all',
                cause: byId('forecastCauseFilter') ? byId('forecastCauseFilter').value : 'all',
                object_category: byId('forecastObjectCategoryFilter') ? byId('forecastObjectCategoryFilter').value : 'all',
                temperature: byId('forecastTemperatureInput') ? byId('forecastTemperatureInput').value : '',
                forecast_days: byId('forecastDaysFilter') ? byId('forecastDaysFilter').value : '',
                history_window: byId('forecastHistoryWindowFilter') ? byId('forecastHistoryWindowFilter').value : ''
            };
        }

        return {
            collectSelectedFilters: collectSelectedFilters,
            getCurrentData: getCurrentData,
            setCurrentData: setCurrentData
        };
    };
})();

