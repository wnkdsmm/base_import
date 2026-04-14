(function (global) {
    var shared = global.FireUi || {};
    var byId = shared.byId;

    global.DashboardState = {
        create: function createDashboardState(options) {
            var initialData = options && options.initialData ? options.initialData : null;

            function getInitialData() {
                return initialData;
            }

            function collectSelectedFilters() {
                return {
                    table_name: byId('tableFilter') ? byId('tableFilter').value : '',
                    year: byId('yearFilter') ? byId('yearFilter').value : 'all',
                    group_column: byId('groupColumnFilter') ? byId('groupColumnFilter').value : ''
                };
            }

            return {
                collectSelectedFilters: collectSelectedFilters,
                getInitialData: getInitialData
            };
        }
    };
}(window));
