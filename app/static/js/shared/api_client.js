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

    global.FireApiClient = {
        apiCall: apiCall,
        createApiError: createApiError,
        getApiErrorMessage: getApiErrorMessage
    };
}(window));
