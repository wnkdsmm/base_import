(function (global) {
    var shared = global.FireUi || {};
    var byId = shared.byId;
    var factory = global.FireStateFactory || {};
    var createStateManager = factory.createStateManager;

    global.MlModelState = {
        create: function createMlModelState(options) {
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

