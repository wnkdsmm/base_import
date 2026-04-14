(function (global) {
    var shared = global.FireUi || {};

    global.ClusteringEvents = {
        init: function initClusteringPage(options) {
            var byId = shared.byId;
            if (!byId) {
                return;
            }

            var form = byId('clusteringForm');
            var tableFilter = byId('clusterTableFilter');
            var retryButton = byId('clusteringRetryButton');
            var initialData = options && options.initialData ? options.initialData : null;

            if (form) {
                form.addEventListener('submit', function (event) {
                    event.preventDefault();
                    if (options && typeof options.onSubmit === 'function') {
                        options.onSubmit();
                    }
                });
            }

            if (form && tableFilter) {
                tableFilter.addEventListener('change', function () {
                    Array.prototype.forEach.call(
                        form.querySelectorAll('input[name="feature_columns"]'),
                        function (field) {
                            field.checked = false;
                        }
                    );
                    if (options && typeof options.onTableChange === 'function') {
                        options.onTableChange();
                    }
                });
            }

            if (retryButton) {
                retryButton.addEventListener('click', function () {
                    if (options && typeof options.onRetry === 'function') {
                        options.onRetry();
                    }
                });
            }

            if (initialData && options && typeof options.onInitialData === 'function') {
                options.onInitialData(initialData);
            }

            if ((!initialData || initialData.bootstrap_mode === 'deferred') && options && typeof options.onDeferredLoad === 'function') {
                options.onDeferredLoad();
            }
        }
    };
}(window));
