(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};

    modules.createForecastingApi = function createForecastingApi(options) {
        var ui = options && options.ui ? options.ui : {};
        var byId = shared.byId;
        var createSingleTimer = shared.createSingleTimer;
        var apiClient = window.FireApiClient || {};
        var apiCall = apiClient.apiCall;
        var pollUntilDone = apiClient.pollUntilDone;
        var getForecastErrorMessage = shared.getErrorMessage;
        var decisionSupportJobPollTimer = createSingleTimer();
        var forecastRequestToken = 0;

        function applyForecastData(data) {
            if (typeof ui.applyForecastData === 'function') {
                ui.applyForecastData(data);
            }
        }

        function hideForecastError() {
            if (typeof ui.hideForecastError === 'function') {
                ui.hideForecastError();
            }
        }

        function showForecastError(message) {
            if (typeof ui.showForecastError === 'function') {
                ui.showForecastError(message);
            }
        }

        function setForecastAsyncVisibility(visible) {
            if (typeof ui.setForecastAsyncVisibility === 'function') {
                ui.setForecastAsyncVisibility(visible);
            }
        }

        function renderForecastJobRuntime(payload) {
            if (typeof ui.renderForecastJobRuntime === 'function') {
                ui.renderForecastJobRuntime(payload);
            }
        }

        function updateDecisionSupportJobState(payload) {
            if (typeof ui.updateDecisionSupportJobState === 'function') {
                ui.updateDecisionSupportJobState(payload);
            }
        }

        function buildForecastRequestQuery(baseQuery, includeDecisionSupport) {
            var params = new URLSearchParams(baseQuery || '');
            params.set('include_decision_support', includeDecisionSupport ? '1' : '0');
            return params.toString();
        }

        async function requestForecastApiPayload(endpoint, query, requestOptions) {
            var result = await apiCall(
                endpoint + '?' + query,
                { headers: { Accept: 'application/json' } },
                'Не удалось выполнить запрос прогноза.',
                'API прогноза вернул ответ в неожиданном формате.'
            );
            var payload = result.payload;
            if (requestOptions && requestOptions.expectResolved && payload && payload.bootstrap_mode === 'deferred') {
                throw new Error('API прогноза вернул стартовую заготовку вместо готового результата.');
            }
            return payload;
        }

        function requestForecastPayload(query, requestOptions) {
            return requestForecastApiPayload('/api/forecasting-data', query, requestOptions);
        }

        function requestForecastMetadataPayload(query, requestOptions) {
            return requestForecastApiPayload('/api/forecasting-metadata', query, requestOptions);
        }

        function buildForecastJobBody(query) {
            var params = new URLSearchParams(query || '');
            return {
                table_name: params.get('table_name') || 'all',
                district: params.get('district') || 'all',
                cause: params.get('cause') || 'all',
                object_category: params.get('object_category') || 'all',
                temperature: params.get('temperature') || '',
                forecast_days: params.get('forecast_days') || '14',
                history_window: params.get('history_window') || 'all'
            };
        }

        function stopDecisionSupportPolling() {
            decisionSupportJobPollTimer.clear();
        }

        function pollDecisionSupportJob(jobId, baseQuery, requestToken) {
            var payload = null;

            if (!jobId) {
                return;
            }

            pollUntilDone(
                '/api/forecasting-decision-support-jobs/' + encodeURIComponent(jobId),
                {
                    requestOptions: { headers: { Accept: 'application/json' } },
                    fallbackMessage: 'Фоновая задача decision support завершилась с ошибкой.'
                },
                {
                    onUpdate: function (nextPayload) {
                        if (requestToken !== forecastRequestToken) {
                            return;
                        }
                        payload = nextPayload;
                        updateDecisionSupportJobState(payload);
                    },
                    onDone: function (donePayload) {
                        if (requestToken !== forecastRequestToken) {
                            return;
                        }
                        applyForecastData(donePayload.result);
                        renderForecastJobRuntime(donePayload);
                        window.history.replaceState({}, '', baseQuery ? '/forecasting?' + baseQuery : '/forecasting');
                    },
                    onError: function (error) {
                        if (requestToken !== forecastRequestToken) {
                            return;
                        }
                        var decisionSupportMessage = getForecastErrorMessage(
                            error,
                            'Не удалось получить статус блока поддержки решений. Попробуйте повторить запрос.'
                        );
                        showForecastError(decisionSupportMessage);
                        renderForecastJobRuntime(payload);
                    }
                },
                {
                    intervalMs: 1200,
                    scheduleNext: function (fn, delay) {
                        decisionSupportJobPollTimer.set(fn, delay);
                    },
                    shouldStop: function () {
                        return requestToken !== forecastRequestToken;
                    },
                    isDone: function (nextPayload) {
                        return Boolean(nextPayload && nextPayload.status === 'completed' && nextPayload.result);
                    },
                    isFailed: function (nextPayload) {
                        return Boolean(nextPayload && (nextPayload.status === 'failed' || nextPayload.status === 'missing'));
                    },
                    getFailureMessage: function (nextPayload) {
                        return nextPayload && nextPayload.error_message
                            ? nextPayload.error_message
                            : 'Фоновая задача decision support завершилась с ошибкой.';
                    }
                }
            );
        }

        async function fetchDecisionSupport(baseQuery, requestToken) {
            var response;
            var payload = null;
            stopDecisionSupportPolling();
            try {
                var result = await apiCall('/api/forecasting-decision-support-jobs', {
                    method: 'POST',
                    headers: {
                        Accept: 'application/json',
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(buildForecastJobBody(baseQuery))
                }, 'Не удалось запустить блок поддержки решений.');
                response = result.response;
                payload = result.payload;
                if (requestToken !== forecastRequestToken) {
                    return;
                }
                updateDecisionSupportJobState(payload);
                if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                    throw new Error(payload && payload.error_message ? payload.error_message : 'Не удалось запустить блок поддержки решений.');
                }
                if (payload.status === 'completed' && payload.result) {
                    applyForecastData(payload.result);
                    renderForecastJobRuntime(payload);
                    return;
                }
                pollDecisionSupportJob(payload.job_id, baseQuery, requestToken);
            } catch (error) {
                if (requestToken !== forecastRequestToken) {
                    return;
                }
                var decisionSupportMessage = getForecastErrorMessage(
                    error,
                    'Не удалось догрузить блок поддержки решений. Базовый прогноз уже показан, можно повторить запрос.'
                );
                showForecastError(decisionSupportMessage);
            }
        }

        async function fetchForecastMetadata(baseQuery, requestToken) {
            try {
                var metadataPayload = await requestForecastMetadataPayload(baseQuery, { expectResolved: false });
                if (requestToken !== forecastRequestToken) {
                    return null;
                }
                applyForecastData(metadataPayload);
                return metadataPayload;
            } catch (error) {
                error.forecastingStage = 'metadata';
                throw error;
            }
        }

        async function fetchForecastData() {
            var form = byId('forecastForm');
            var button = byId('forecastRefreshButton');
            if (!form) {
                return;
            }

            var requestToken = forecastRequestToken + 1;
            var baseQuery = new URLSearchParams(new FormData(form)).toString();
            var query = buildForecastRequestQuery(baseQuery, false);
            forecastRequestToken = requestToken;
            stopDecisionSupportPolling();
            hideForecastError();
            setForecastAsyncVisibility(false);
            renderForecastJobRuntime(null);
            if (button) {
                button.disabled = true;
            }

            try {
                await fetchForecastMetadata(baseQuery, requestToken);
                if (requestToken !== forecastRequestToken) {
                    return;
                }

                var data = await requestForecastPayload(query, { expectResolved: true });
                if (requestToken !== forecastRequestToken) {
                    return;
                }
                applyForecastData(data);
                window.history.replaceState({}, '', baseQuery ? '/forecasting?' + baseQuery : '/forecasting');
                if (data.decision_support_pending) {
                    void fetchDecisionSupport(baseQuery, requestToken);
                }
            } catch (error) {
                if (requestToken !== forecastRequestToken) {
                    return;
                }
                var forecastErrorMessage = getForecastErrorMessage(
                    error,
                    'Не удалось загрузить базовый прогноз. Попробуйте изменить фильтры или запустить расчёт еще раз.'
                );
                if (error && error.forecastingStage === 'metadata') {
                    showForecastError(forecastErrorMessage);
                    return;
                }
                showForecastError(forecastErrorMessage);
            } finally {
                if (button && requestToken === forecastRequestToken) {
                    button.disabled = false;
                }
            }
        }

        function startDecisionSupportFromQuery(baseQuery) {
            forecastRequestToken += 1;
            void fetchDecisionSupport(baseQuery, forecastRequestToken);
            return forecastRequestToken;
        }

        return {
            buildForecastRequestQuery: buildForecastRequestQuery,
            fetchForecastData: fetchForecastData,
            fetchForecastMetadata: fetchForecastMetadata,
            fetchDecisionSupport: fetchDecisionSupport,
            getRequestToken: function () {
                return forecastRequestToken;
            },
            pollDecisionSupportJob: pollDecisionSupportJob,
            requestForecastMetadataPayload: requestForecastMetadataPayload,
            requestForecastPayload: requestForecastPayload,
            startDecisionSupportFromQuery: startDecisionSupportFromQuery,
            stopDecisionSupportPolling: stopDecisionSupportPolling
        };
    };
})();

