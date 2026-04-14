(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};
    var byId = shared.byId;

    modules.createForecastingState = function createForecastingState(options) {
        var currentData = options && options.initialData ? options.initialData : null;

        function setCurrentData(data) {
            currentData = data || null;
            return currentData;
        }

        function getCurrentData() {
            return currentData;
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

