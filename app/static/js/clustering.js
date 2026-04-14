(function (global) {
    var shared = global.FireUi;
    if (!shared || !global.ClusteringState || !global.ClusteringRender || !global.ClusteringEvents) {
        return;
    }

    var byId = shared.byId;
    var fetchJson = shared.fetchJson;
    var renderApi = global.ClusteringRender.create();
    var state = global.ClusteringState.create({
        initialData: global.__FIRE_CLUSTERING_INITIAL__ || null
    });

var getClusteringErrorMessage = shared.getErrorMessage;

    function buildClusteringJobBody(form) {
        var formData = new FormData(form);
        return {
            table_name: String(formData.get('table_name') || ''),
            cluster_count: String(formData.get('cluster_count') || '4'),
            sample_limit: String(formData.get('sample_limit') || '1000'),
            sampling_strategy: String(formData.get('sampling_strategy') || 'stratified'),
            feature_columns: formData.getAll('feature_columns').map(function (value) {
                return String(value || '').trim();
            }).filter(Boolean)
        };
    }

    function stopClusteringJobPolling() {
        state.clearPollTimer();
    }

    

async function pollClusteringJob(jobId, query) {
        var response;
        var payload = null;
        if (!jobId) {
            return;
        }

        try {
            var result = await fetchJson('/api/clustering-jobs/' + encodeURIComponent(jobId), {
                headers: { Accept: 'application/json' }
            }, 'Фоновая clustering-задача завершилась с ошибкой.');
            response = result.response;
            payload = result.payload;
            renderApi.updateClusteringAsyncStateForJob(payload);

            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Фоновая clustering-задача завершилась с ошибкой.');
            }
            if (payload.status === 'completed' && payload.result) {
                renderApi.applyClusteringData(payload.result);
                window.history.replaceState({}, '', query ? '/clustering?' + query : '/clustering');
                return;
            }

            state.setPollTimer(function () {
                pollClusteringJob(jobId, query);
            }, 1200);
        } catch (error) {
            console.error(error);
            renderApi.showClusteringError(getClusteringErrorMessage(
                error,
                'Не удалось получить статус clustering-задачи. Попробуйте повторить расчёт ещё раз.'
            ));
        }
    }

    

async function fetchClusteringData() {
        var form = byId('clusteringForm');
        var button = byId('clusterRefreshButton');
        var body;
        if (!form) {
            return;
        }

        var params = new URLSearchParams(new FormData(form));
        var query = params.toString();
        body = buildClusteringJobBody(form);
        if (button) {
            button.disabled = true;
        }
        stopClusteringJobPolling();
        renderApi.hideClusteringError();
        renderApi.renderClusteringJobRuntime(null);

        try {
            var result = await fetchJson('/api/clustering-jobs', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            }, 'Не удалось выполнить запрос clustering.', 'От clustering API пришел не JSON-ответ.');
            var data = result.payload;
            if (data && data.bootstrap_mode === 'deferred' && !data.has_data) {
                throw new Error('В clustering API вернулся shell-пейлоад вместо готовых данных.');
            }
            renderApi.updateClusteringAsyncStateForJob(data);
            if (data.status === 'completed' && data.result) {
                renderApi.applyClusteringData(data.result);
                window.history.replaceState({}, '', query ? '/clustering?' + query : '/clustering');
                return;
            }
            pollClusteringJob(data.job_id, query);
        } catch (error) {
            var clusteringErrorMessage = getClusteringErrorMessage(
                error,
                'Не удалось получить данные кластеризации. Попробуйте повторить расчёт еще раз.'
            );
            console.error(error);
            renderApi.showClusteringError(clusteringErrorMessage);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    function init() {
        global.ClusteringEvents.init({
            initialData: state.getInitialData(),
            onDeferredLoad: fetchClusteringData,
            onInitialData: function (data) {
                renderApi.applyClusteringData(data);
            },
            onRetry: fetchClusteringData,
            onSubmit: fetchClusteringData,
            onTableChange: fetchClusteringData
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
        return;
    }
    init();
}(window));
