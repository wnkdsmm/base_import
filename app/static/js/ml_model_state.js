(function (global) {
    var shared = global.FireUi || {};
    var byId = shared.byId;

    global.MlModelState = {
        create: function createMlModelState(options) {
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
                    table_name: byId('mlTableFilter') ? byId('mlTableFilter').value : 'all',
                    cause: byId('mlCauseFilter') ? byId('mlCauseFilter').value : 'all',
                    object_category: byId('mlObjectCategoryFilter') ? byId('mlObjectCategoryFilter').value : 'all',
                    temperature: byId('mlTemperatureInput') ? byId('mlTemperatureInput').value : '',
                    forecast_days: byId('mlForecastDaysFilter') ? byId('mlForecastDaysFilter').value : '14',
                    history_window: byId('mlHistoryWindowFilter') ? byId('mlHistoryWindowFilter').value : 'all'
                };
            }

            return {
                collectSelectedFilters: collectSelectedFilters,
                getCurrentData: getCurrentData,
                setCurrentData: setCurrentData
            };
        }
    };
}(window));

