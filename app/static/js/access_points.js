(function (global) {
    var shared = global.FireUi;
    if (!shared || !global.AccessPointsRender || !global.AccessPointsState || !global.AccessPointsEvents) {
        return;
    }

    var byId = shared.byId;
    var renderApi = global.AccessPointsRender.create();
    var state = global.AccessPointsState.create({
        initialData: global.__FIRE_ACCESS_POINTS_INITIAL__ || {}
    });

function getFormParams() {
        var form = byId('accessPointsForm');
        var formData = form ? new FormData(form) : new FormData();
        return {
            table_name: String(formData.get('table_name') || 'all'),
            district: String(formData.get('district') || 'all'),
            year: String(formData.get('year') || 'all'),
            limit: String(formData.get('limit') || '25'),
            feature_columns: formData.getAll('feature_columns').map(function (value) {
                return String(value || '').trim();
            }).filter(Boolean)
        };
    }

    function buildAccessPointsQuery(params) {
        var query = new URLSearchParams();
        query.set('table_name', params.table_name || 'all');
        query.set('district', params.district || 'all');
        query.set('year', params.year || 'all');
        query.set('limit', params.limit || '25');
        (Array.isArray(params.feature_columns) ? params.feature_columns : []).forEach(function (value) {
            query.append('feature_columns', value);
        });
        return query;
    }

    function updateUrl(params) {
        var query = buildAccessPointsQuery(params);
        window.history.replaceState({}, '', '/access-points?' + query.toString());
    }

    
    function validateFeatureSelection(params) {
        if (Array.isArray(params.feature_columns) && params.feature_columns.length) {
            return true;
        }
        renderApi.showError('Выберите хотя бы один фактор scoring, чтобы пересчитать рейтинг проблемных точек.');
        return false;
    }

    async function fetchAccessPoints(params) {
        if (!validateFeatureSelection(params)) {
            return;
        }

        var requestId = state.nextRequestId();
        var activeController = state.getCurrentController();
        if (activeController) {
            activeController.abort();
        }
        var controller = new AbortController();
        state.setCurrentController(controller);
        var refreshButton = byId('accessPointsRefreshButton');
        if (refreshButton) {
            refreshButton.disabled = true;
        }
        renderApi.showLoading('Собираем incidents по точкам, считаем explainable score, decomposition и uncertainty notes.');
        try {
            var query = buildAccessPointsQuery(params);
            var result = await shared.fetchJson('/api/access-points-data?' + query.toString(), {
                headers: { Accept: 'application/json' },
                signal: controller.signal
            }, 'Не удалось построить рейтинг проблемных точек.');
            var payload = result.payload;
            if (!state.isLatestRequest(requestId)) {
                return;
            }
            renderApi.render(payload);
            updateUrl(params);
        } catch (error) {
            if (error && error.name === 'AbortError') {
                return;
            }
            if (!state.isLatestRequest(requestId)) {
                return;
            }
            renderApi.showError(error && error.message ? error.message : 'Не удалось построить рейтинг проблемных точек.');
        } finally {
            state.clearController(controller);
            if (refreshButton && !state.getCurrentController()) {
                refreshButton.disabled = false;
            }
        }
    }

    function bootstrap() {
        var initialData = state.getInitialData() || {};
        if (initialData.bootstrap_mode === 'deferred') {
            renderApi.showLoading();
            fetchAccessPoints(getFormParams());
        } else {
            renderApi.renderCharts(initialData.charts || {});
            if (shared.revealPageContent) { shared.revealPageContent(); }
        }
    }

    function init() {
        global.AccessPointsEvents.init({
            onBootstrap: bootstrap,
            onRetry: function () {
                fetchAccessPoints(getFormParams());
            },
            onSubmit: function () {
                fetchAccessPoints(getFormParams());
            }
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
        return;
    }
    init();
}(window));
