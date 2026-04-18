(function (global) {
    var shared = global.FireUi || {};
    var byId = shared.byId;
    var createSingleTimer = shared.createSingleTimer;
    var apiClient = global.FireApiClient || {};
    var apiCall = apiClient.apiCall;
    var pollUntilDone = apiClient.pollUntilDone;
    var getErrorMessage = shared.getErrorMessage;

    var jobPollTimer = createSingleTimer();
    var currentJobState = null;
    var isFetching = false;

    function withHandlers(handlers) {
        return handlers || {};
    }

    function notifyBusy(handlers, busy) {
        isFetching = !!busy;
        if (handlers && typeof handlers.onBusyChange === 'function') {
            handlers.onBusyChange(isFetching);
        }
    }

    function notifyJobState(handlers, payload) {
        currentJobState = payload || null;
        if (handlers && typeof handlers.onJobState === 'function') {
            handlers.onJobState(currentJobState);
        }
    }

    function notifyCompleted(handlers, result, payload) {
        if (handlers && typeof handlers.onCompleted === 'function') {
            handlers.onCompleted(result, payload || currentJobState);
        }
    }

    function notifyError(handlers, error, fallbackMessage) {
        if (handlers && typeof handlers.onError === 'function') {
            handlers.onError(getErrorMessage(error, fallbackMessage), error);
        }
    }

    function buildQueryFromForm(formId) {
        var form = byId(formId || 'mlModelForm');
        if (!form) {
            return '';
        }
        return new URLSearchParams(new FormData(form)).toString();
    }

    function buildPayloadFromQuery(query) {
        var params = new URLSearchParams(query || '');
        return {
            table_name: params.get('table_name') || 'all',
            cause: params.get('cause') || 'all',
            object_category: params.get('object_category') || 'all',
            temperature: params.get('temperature') || '',
            forecast_days: params.get('forecast_days') || '14',
            history_window: params.get('history_window') || 'all'
        };
    }

    function buildRequestPayload(options) {
        var settings = options || {};
        var query = settings.useLocationSearch && global.location.search
            ? global.location.search.replace(/^\?/, '')
            : buildQueryFromForm(settings.formId || 'mlModelForm');
        return {
            body: buildPayloadFromQuery(query),
            query: query
        };
    }

    function stopJobPolling() {
        jobPollTimer.clear();
    }

    async function pollMlJob(jobId, handlers) {
        var callbacks = withHandlers(handlers);

        if (!jobId) {
            notifyBusy(callbacks, false);
            notifyError(callbacks, new Error('Не передан идентификатор ML-задачи.'), 'Не удалось получить статус ML-задачи.');
            return;
        }

        pollUntilDone(
            '/api/ml-model-jobs/' + encodeURIComponent(jobId),
            {
                requestOptions: { headers: { Accept: 'application/json' } },
                fallbackMessage: 'Фоновая ML-задача завершилась с ошибкой.'
            },
            {
                onUpdate: function (payload) {
                    notifyJobState(callbacks, payload);
                },
                onDone: function (payload) {
                    notifyBusy(callbacks, false);
                    notifyCompleted(callbacks, payload.result, payload);
                },
                onError: function (error) {
                    notifyBusy(callbacks, false);
                    notifyError(callbacks, error, 'Не удалось получить статус ML-задачи.');
                }
            },
            {
                intervalMs: 1200,
                scheduleNext: function (fn, delay) {
                    jobPollTimer.set(fn, delay);
                },
                isDone: function (payload) {
                    return Boolean(payload && payload.status === 'completed' && payload.result);
                },
                isFailed: function (payload) {
                    return Boolean(payload && (payload.status === 'failed' || payload.status === 'missing'));
                },
                getFailureMessage: function (payload) {
                    return payload && payload.error_message
                        ? payload.error_message
                        : 'Фоновая ML-задача завершилась с ошибкой.';
                }
            }
        );
    }

    async function startMlModelJob(options, handlers) {
        var settings = options || {};
        var callbacks = withHandlers(handlers);
        var requestPayload = buildRequestPayload(settings);

        stopJobPolling();
        currentJobState = null;
        notifyBusy(callbacks, true);
        if (typeof callbacks.onStart === 'function') {
            callbacks.onStart(requestPayload, settings);
        }

        try {
            var result = await apiCall('/api/ml-model-jobs', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(requestPayload.body)
            }, 'Не удалось запустить ML-задачу.');
            var payload = result.payload;
            notifyJobState(callbacks, payload);

            global.history.replaceState(
                {},
                '',
                requestPayload.query ? (global.location.pathname + '?' + requestPayload.query) : global.location.pathname
            );

            if (payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Не удалось запустить ML-задачу.');
            }

            if (payload.status === 'completed' && payload.result) {
                notifyBusy(callbacks, false);
                notifyCompleted(callbacks, payload.result, payload);
                return;
            }

            pollMlJob(payload.job_id, callbacks);
        } catch (error) {
            notifyBusy(callbacks, false);
            notifyError(callbacks, error, 'Не удалось запустить ML-анализ. Попробуйте еще раз.');
        }
    }

    global.MlModelApi = {
        buildPayloadFromQuery: buildPayloadFromQuery,
        buildQueryFromForm: buildQueryFromForm,
        buildRequestPayload: buildRequestPayload,
        getCurrentJobState: function () {
            return currentJobState;
        },
        isFetching: function () {
            return isFetching;
        },
        pollMlJob: pollMlJob,
        startMlModelJob: startMlModelJob,
        stopJobPolling: stopJobPolling
    };
}(window));
