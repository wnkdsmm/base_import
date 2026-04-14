(function (global) {
    var shared = global.FireUi || {};

    global.DashboardEvents = {
        init: function initDashboardPage(options) {
            var byId = shared.byId;
            if (!byId) {
                return;
            }

            var form = byId('filtersForm');
            var tableSelect = byId('tableFilter');
            var yearSelect = byId('yearFilter');
            var groupColumnSelect = byId('groupColumnFilter');

            [tableSelect, yearSelect, groupColumnSelect].forEach(function (selectNode) {
                if (selectNode) {
                    selectNode.addEventListener('change', function () {
                        if (options && typeof options.onFilterChange === 'function') {
                            options.onFilterChange();
                        }
                    });
                }
            });

            if (form) {
                form.addEventListener('submit', function (event) {
                    event.preventDefault();
                    if (options && typeof options.onSubmit === 'function') {
                        options.onSubmit();
                    }
                });
            }

            var retryButton = byId('dashboardInlineRetryButton');
            if (retryButton) {
                retryButton.addEventListener('click', function () {
                    if (options && typeof options.onRetry === 'function') {
                        options.onRetry();
                    }
                });
            }

            if (options && typeof options.onBootstrap === 'function') {
                options.onBootstrap();
            }
        }
    };
}(window));
