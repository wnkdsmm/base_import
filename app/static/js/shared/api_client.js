(function (global) {
    function getApiErrorMessage(payload, fallback) {
        var normalizedFallback = fallback || 'Request failed.';
        if (!payload || typeof payload !== 'object') {
            return normalizedFallback;
        }

        if (payload.error && typeof payload.error === 'object') {
            var apiMessage = String(payload.error.message || payload.error.detail || payload.error.code || '').trim();
            if (apiMessage) {
                return apiMessage;
            }
        }

        var legacyMessage = String(payload.error_message || payload.detail || payload.message || '').trim();
        return legacyMessage || normalizedFallback;
    }

    function createApiError(response, payload, fallback) {
        var error = new Error(getApiErrorMessage(payload, fallback));
        error.status = response && typeof response.status === 'number' ? response.status : 0;
        error.payload = payload || null;
        return error;
    }

    async function apiCall(url, options, fallback, invalidJsonFallback) {
        var response = await fetch(url, options || {});
        var payload;
        try {
            payload = await response.json();
        } catch (error) {
            if (!response.ok) {
                throw createApiError(response, null, invalidJsonFallback || fallback);
            }
            throw error;
        }

        if (!response.ok || (payload && payload.ok === false)) {
            throw createApiError(response, payload, fallback);
        }

        return {
            payload: payload,
            response: response
        };
    }

    function pollUntilDone(endpoint, params, callbacks, options) {
        var requestParams = params || {};
        var handlers = callbacks || {};
        var settings = options || {};
        var intervalMs = typeof settings.intervalMs === 'number' ? settings.intervalMs : 1200;
        var timeoutMs = typeof settings.timeoutMs === 'number' ? settings.timeoutMs : 0;
        var scheduleNext = typeof settings.scheduleNext === 'function'
            ? settings.scheduleNext
            : function (fn, delay) { return setTimeout(fn, delay); };
        var shouldStop = typeof settings.shouldStop === 'function'
            ? settings.shouldStop
            : function () { return false; };
        var isDone = typeof settings.isDone === 'function'
            ? settings.isDone
            : function (payload) { return Boolean(payload && payload.status === 'completed' && payload.result); };
        var isFailed = typeof settings.isFailed === 'function'
            ? settings.isFailed
            : function (payload) { return Boolean(payload && (payload.status === 'failed' || payload.status === 'missing')); };
        var getFailureMessage = typeof settings.getFailureMessage === 'function'
            ? settings.getFailureMessage
            : function (payload) {
                return (payload && payload.error_message) || requestParams.fallbackMessage || 'Background job failed.';
            };

        function run(attempt, startedAt) {
            if (shouldStop()) {
                return;
            }

            apiCall(
                endpoint,
                requestParams.requestOptions || { headers: { Accept: 'application/json' } },
                requestParams.fallbackMessage,
                requestParams.invalidJsonFallback
            ).then(function (result) {
                if (shouldStop()) {
                    return;
                }

                var payload = result.payload;
                var response = result.response;
                if (typeof handlers.onUpdate === 'function') {
                    handlers.onUpdate(payload, response, attempt);
                }

                if (isFailed(payload, response)) {
                    throw new Error(getFailureMessage(payload, response));
                }

                if (isDone(payload, response)) {
                    if (typeof handlers.onDone === 'function') {
                        handlers.onDone(payload, response, attempt);
                    }
                    return;
                }

                if (timeoutMs > 0 && (Date.now() - startedAt) >= timeoutMs) {
                    throw new Error(requestParams.timeoutMessage || requestParams.fallbackMessage || 'Polling timeout.');
                }

                scheduleNext(function () {
                    run(attempt + 1, startedAt);
                }, intervalMs);
            }).catch(function (error) {
                if (shouldStop()) {
                    return;
                }
                if (typeof handlers.onError === 'function') {
                    handlers.onError(error);
                }
            });
        }

        run(0, Date.now());
    }

    global.FireApiClient = {
        apiCall: apiCall,
        createApiError: createApiError,
        getApiErrorMessage: getApiErrorMessage,
        pollUntilDone: pollUntilDone
    };
}(window));
