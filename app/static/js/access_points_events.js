(function (global) {
    var shared = global.FireUi || {};

    global.AccessPointsEvents = {
        init: function initAccessPointsPage(options) {
            var byId = shared.byId;
            if (!byId) {
                return;
            }

            var form = byId('accessPointsForm');
            if (form) {
                form.addEventListener('submit', function (event) {
                    event.preventDefault();
                    if (options && typeof options.onSubmit === 'function') {
                        options.onSubmit();
                    }
                });
            }

            var retryButton = byId('accessPointsRetryButton');
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
