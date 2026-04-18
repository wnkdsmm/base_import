(function (global) {
    var shared = global.FireUi;
    if (!shared || !global.DashboardState || !global.DashboardRender || !global.DashboardEvents) {
        return;
    }

    var byId = shared.byId;
    var fetchJson = shared.fetchJson;
    var getApiErrorMessage = shared.getApiErrorMessage;
    var renderApi = global.DashboardRender.create();
    var state = global.DashboardState.create({
        initialData: global.__FIRE_DASHBOARD_INITIAL_DATA__ || null
    });

function buildDashboardApiError(response, payload) {
        const errorPayload = payload && payload.error ? payload.error : {};
        const statusCode = Number(errorPayload.status_code || (response && response.status) || 0);
        const fallbackMessage = (
            statusCode >= 500
                ? 'Не удалось обновить dashboard. Попробуйте повторить запрос.'
                : 'Не удалось обработать параметры dashboard.'
        );
        const message = getApiErrorMessage(payload, fallbackMessage);
        const error = new Error(message);
        error.dashboardStatusCode = statusCode;
        error.dashboardErrorId = errorPayload.error_id || '';
        error.dashboardCode = errorPayload.code || '';
        return error;
    }

    function readDashboardPayload(payload) {
        if (!payload || typeof payload !== 'object') {
            const contractError = new Error('Dashboard API вернул пустой ответ.');
            contractError.dashboardStatusCode = 502;
            throw contractError;
        }

        if (payload.bootstrap_mode === 'deferred') {
            const contractError = new Error('Dashboard API вернул shell вместо готовых данных.');
            contractError.dashboardStatusCode = 502;
            throw contractError;
        }

        return payload;
    }

    async function fetchDashboardPayload(url, options) {
        try {
            const result = await fetchJson(
                url,
                options,
                'Не удалось обновить dashboard. Попробуйте повторить запрос.'
            );
            return readDashboardPayload(result.payload);
        } catch (error) {
            if (error instanceof SyntaxError) {
                const contractError = new Error('Dashboard API вернул пустой ответ.');
                contractError.dashboardStatusCode = 502;
                throw contractError;
            }
            if (error && Object.prototype.hasOwnProperty.call(error, 'payload')) {
                throw buildDashboardApiError(error, error.payload);
            }
            throw error;
        }
    }

async function fetchDashboardData() {
        const form = byId('filtersForm');
        const button = byId('refreshDashboardButton');
        if (!form) {
            return;
        }

        const params = new URLSearchParams(new FormData(form));
        const query = params.toString();

        if (button) {
            button.disabled = true;
        }

        try {
            renderApi.hideDashboardError();
            const data = await fetchDashboardPayload('/api/dashboard-data?' + query, {
                headers: {
                    'Accept': 'application/json'
                }
            });
            renderApi.applyDashboardData(data);
            window.history.replaceState({}, '', renderApi.buildDashboardPageHref(state.collectSelectedFilters()));
        } catch (error) {
            console.error(error);
            renderApi.showDashboardError(error);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    function syncBriefLink() {
        var filters = state.collectSelectedFilters();
        renderApi.renderFilterSummary();
        renderApi.updateDashboardBriefExport(filters);
        renderApi.updateDashboardScreenLinks(filters);
    }

    function bootstrap() {
        var initialData = state.getInitialData();
        var isDeferredBootstrap = !!(initialData && initialData.bootstrap_mode === 'deferred');
        var shouldFetchOnLoad = !initialData || isDeferredBootstrap;

        if (initialData && !isDeferredBootstrap) {
            renderApi.renderDashboardCharts(initialData.charts || {});
            if (shared.revealPageContent) { shared.revealPageContent(); }
        } else {
            syncBriefLink();
        }

        if (shouldFetchOnLoad) {
            fetchDashboardData();
        }

        global.fireDashboard = {
            reload: fetchDashboardData,
            afterImport: function () {
                fetchDashboardData();
            }
        };
    }

    function init() {
        global.DashboardEvents.init({
            onBootstrap: bootstrap,
            onFilterChange: syncBriefLink,
            onRetry: fetchDashboardData,
            onSubmit: fetchDashboardData
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
        return;
    }
    init();
}(window));
