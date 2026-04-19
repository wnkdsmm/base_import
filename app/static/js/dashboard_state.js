(function (global) {
    var shared = global.FireUi || {};
    var byId = shared.byId;
    var factory = global.FireStateFactory || {};
    var createModuleState = factory.createModuleState;

    global.DashboardState = {
        create: function createDashboardState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var state = typeof createModuleState === 'function'
                ? createModuleState('dashboard', { initialData: initialData })
                : null;
            var fallbackInitialData = initialData;

            function getInitialData() {
                return state ? state.get('initialData') : fallbackInitialData;
            }

            function collectSelectedFilters() {
                return {
                    table_name: byId('tableFilter') ? byId('tableFilter').value : '',
                    year: byId('yearFilter') ? byId('yearFilter').value : 'all',
                    group_column: byId('groupColumnFilter') ? byId('groupColumnFilter').value : '',
                    horizon_days: byId('horizonDaysFilter') ? byId('horizonDaysFilter').value : '14'
                };
            }

            return {
                collectSelectedFilters: collectSelectedFilters,
                getInitialData: getInitialData
            };
        }
    };
}(window));
