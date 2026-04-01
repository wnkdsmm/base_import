// API and jobs

    function getForecastApiErrorMessage(payload, fallback) {
        var normalizedFallback = fallback || 'РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ Р·Р°РїСЂРѕСЃ РїСЂРѕРіРЅРѕР·Р°.';
        if (!payload || typeof payload !== 'object') {
            return normalizedFallback;
        }

        if (payload.error && typeof payload.error === 'object') {
            var apiMessage = String(payload.error.message || payload.error.detail || payload.error.code || '').trim();
            if (apiMessage) {
                return apiMessage;
            }
        }

        var legacyMessage = String(payload.detail || payload.message || '').trim();
        return legacyMessage || normalizedFallback;
    }

    function createForecastApiError(response, payload, fallback) {
        var error = new Error(getForecastApiErrorMessage(payload, fallback));
        error.status = response && typeof response.status === 'number' ? response.status : 0;
        error.payload = payload || null;
        return error;
    }

    function getForecastErrorMessage(error, fallback) {
        var message = error && typeof error.message === 'string' ? error.message.trim() : '';
        return message || fallback;
    }

    async function requestForecastApiPayload(endpoint, query, options) {
        var response = await fetch(endpoint + '?' + query, { headers: { Accept: 'application/json' } });
        var payload;
        try {
            payload = await response.json();
        } catch (error) {
            if (!response.ok) {
                throw createForecastApiError(response, null, 'API РїСЂРѕРіРЅРѕР·Р° РІРµСЂРЅСѓР» РѕС‚РІРµС‚ РІ РЅРµРѕР¶РёРґР°РЅРЅРѕРј С„РѕСЂРјР°С‚Рµ.');
            }
            throw error;
        }
        if (!response.ok) {
            throw createForecastApiError(response, payload, 'РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ Р·Р°РїСЂРѕСЃ РїСЂРѕРіРЅРѕР·Р°.');
        }
        if (payload && payload.ok === false) {
            throw createForecastApiError(response, payload, 'РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ Р·Р°РїСЂРѕСЃ РїСЂРѕРіРЅРѕР·Р°.');
        }
        if (options && options.expectResolved && payload && payload.bootstrap_mode === 'deferred') {
            throw new Error('API РїСЂРѕРіРЅРѕР·Р° РІРµСЂРЅСѓР» СЃС‚Р°СЂС‚РѕРІСѓСЋ Р·Р°РіРѕС‚РѕРІРєСѓ РІРјРµСЃС‚Рѕ РіРѕС‚РѕРІРѕРіРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°.');
        }
        return payload;
    }

    function requestForecastPayload(query, options) {
        return requestForecastApiPayload('/api/forecasting-data', query, options);
    }

    function requestForecastMetadataPayload(query, options) {
        return requestForecastApiPayload('/api/forecasting-metadata', query, options);
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

    function renderForecastJobRuntime(jobPayload) {
        var runtimeNode = byId('forecastJobRuntime');
        var statusNode = byId('forecastJobStatusLabel');
        var metaNode = byId('forecastJobMeta');
        var logsNode = byId('forecastJobLogOutput');
        var safeJob = jobPayload || {};
        var logs = Array.isArray(safeJob.logs) ? safeJob.logs : [];
        var meta = safeJob.meta || {};
        var metaParts = [];

        if (!runtimeNode || !statusNode || !metaNode || !logsNode) {
            return;
        }
        if (!safeJob.job_id) {
            runtimeNode.classList.add('is-hidden');
            runtimeNode.classList.remove('is-ready');
            statusNode.textContent = '';
            metaNode.textContent = '';
            logsNode.textContent = '';
            return;
        }

        runtimeNode.classList.remove('is-hidden');
        runtimeNode.classList.toggle('is-ready', safeJob.status === 'completed');
        statusNode.textContent = 'РЎС‚Р°С‚СѓСЃ decision-support job: ' + String(safeJob.status || 'pending');
        metaParts.push('job_id: ' + String(safeJob.job_id || ''));
        if (meta.cache_hit) {
            metaParts.push('РєСЌС€');
        }
        if (safeJob.reused) {
            metaParts.push('РїРµСЂРµРёСЃРїРѕР»СЊР·РѕРІР°РЅ');
        }
        metaNode.textContent = metaParts.join(' | ');
        logsNode.textContent = logs.length ? logs.join('\n') : 'Р›РѕРіРё РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ Р·Р°РїСѓСЃРєР° С„РѕРЅРѕРІРѕР№ Р·Р°РґР°С‡Рё.';
    }

    function updateDecisionSupportJobState(jobPayload) {
        var safeJob = jobPayload || {};
        var meta = safeJob.meta || {};
        var logs = Array.isArray(safeJob.logs) ? safeJob.logs : [];
        var stageLabel = meta.stage_label ? String(meta.stage_label) : 'РџРѕРґРґРµСЂР¶РєР° СЂРµС€РµРЅРёР№';
        var message = String(meta.stage_message || '').trim();

        if (!message && logs.length) {
            message = logs[logs.length - 1];
        }
        if (!message) {
            message = safeJob.status === 'pending'
                ? 'РћР¶РёРґР°РµРј Р·Р°РїСѓСЃРєР° С„РѕРЅРѕРІРѕРіРѕ СЂР°СЃС‡РµС‚Р° Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.'
                : 'Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ РІ С„РѕРЅРµ.';
        }

        renderForecastJobRuntime(safeJob);
        if (safeJob.status === 'completed') {
            setDecisionSupportStatus('Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ Рё СЂРµРєРѕРјРµРЅРґР°С†РёРё РіРѕС‚РѕРІС‹.', 'ready');
            return;
        }

        setForecastLoadingState(
            'Р”РѕРіСЂСѓР¶Р°РµРј РїРѕРґРґРµСЂР¶РєСѓ СЂРµС€РµРЅРёР№',
            message,
            3,
            { showSkeleton: false }
        );
        setForecastProgress(3, {
            lead: 'Decision support: ' + stageLabel,
            message: message
        });
        setDecisionSupportStatus(message, safeJob.status === 'failed' ? 'error' : 'pending');
    }

    async function pollDecisionSupportJob(jobId, baseQuery, requestToken) {
        var response;
        var payload = null;

        if (!jobId) {
            return;
        }

        try {
            response = await fetch('/api/forecasting-decision-support-jobs/' + encodeURIComponent(jobId), {
                headers: { Accept: 'application/json' }
            });
            payload = await response.json();
            if (requestToken !== forecastRequestToken) {
                return;
            }
            updateDecisionSupportJobState(payload);

            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Р¤РѕРЅРѕРІР°СЏ Р·Р°РґР°С‡Р° decision support Р·Р°РІРµСЂС€РёР»Р°СЃСЊ СЃ РѕС€РёР±РєРѕР№.');
            }
            if (payload.status === 'completed' && payload.result) {
                applyForecastData(payload.result);
                renderForecastJobRuntime(payload);
                window.history.replaceState({}, '', baseQuery ? '/forecasting?' + baseQuery : '/forecasting');
                return;
            }

            decisionSupportJobPollTimer = setTimeout(function () {
                pollDecisionSupportJob(jobId, baseQuery, requestToken);
            }, 1200);
        } catch (error) {
            if (requestToken !== forecastRequestToken) {
                return;
            }
            var decisionSupportMessage = getForecastErrorMessage(
                error,
                'РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ СЃС‚Р°С‚СѓСЃ Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ.'
            );
            console.error(error);
            showForecastError(decisionSupportMessage);
            clearForecastStepTimers();
            setForecastProgress(3, {
                lead: 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РІРµСЂС€РёС‚СЊ РїРѕРґРґРµСЂР¶РєСѓ СЂРµС€РµРЅРёР№',
                message: decisionSupportMessage,
                isError: true
            });
            setDecisionSupportStatus(decisionSupportMessage, 'error');
            renderForecastJobRuntime(payload);
        }
    }


    async function fetchDecisionSupport(baseQuery, requestToken) {
        var response;
        var payload = null;
        clearForecastStepTimers();
        stopDecisionSupportPolling();
        setForecastLoadingState(
            'Р”РѕРіСЂСѓР¶Р°РµРј РїРѕРґРґРµСЂР¶РєСѓ СЂРµС€РµРЅРёР№',
            'Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· СѓР¶Рµ РѕР±РЅРѕРІР»С‘РЅ. РџРѕРґС‚СЏРіРёРІР°РµРј СЂРµРєРѕРјРµРЅРґР°С†РёРё Рё С„РёРЅР°Р»СЊРЅС‹Рµ РІРёР·СѓР°Р»РёР·Р°С†РёРё.',
            3,
            { showSkeleton: false }
        );
        setDecisionSupportStatus('Р”РѕРіСЂСѓР¶Р°РµРј РїСЂРёРѕСЂРёС‚РµС‚С‹ С‚РµСЂСЂРёС‚РѕСЂРёР№ Рё СЂРµРєРѕРјРµРЅРґР°С†РёРё...', 'pending');
        try {
            response = await fetch('/api/forecasting-decision-support-jobs', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(buildForecastJobBody(baseQuery))
            });
            payload = await response.json();
            if (requestToken !== forecastRequestToken) {
                return;
            }
            updateDecisionSupportJobState(payload);
            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ Р±Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.');
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
                'РќРµ СѓРґР°Р»РѕСЃСЊ РґРѕРіСЂСѓР·РёС‚СЊ Р±Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№. Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· СѓР¶Рµ РїРѕРєР°Р·Р°РЅ, РјРѕР¶РЅРѕ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ.'
            );
            console.error(error);
            showForecastError(decisionSupportMessage);
            clearForecastStepTimers();
            setForecastProgress(3, {
                lead: 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РІРµСЂС€РёС‚СЊ РїРѕРґРґРµСЂР¶РєСѓ СЂРµС€РµРЅРёР№',
                message: decisionSupportMessage,
                isError: true
            });
            setDecisionSupportStatus(decisionSupportMessage, 'error');
        }
    }

    async function fetchForecastMetadata(baseQuery, requestToken) {
        setForecastLoadingState(
            'Р—Р°РіСЂСѓР¶Р°РµРј С„РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё',
            'РЎРЅР°С‡Р°Р»Р° РґРѕРіСЂСѓР¶Р°РµРј С„РёР»СЊС‚СЂС‹, РґРѕСЃС‚СѓРїРЅС‹Рµ Р·РЅР°С‡РµРЅРёСЏ Рё РЅР°Р№РґРµРЅРЅС‹Рµ РїСЂРёР·РЅР°РєРё.',
            0,
            { showSkeleton: true }
        );
        startForecastProgressSequence();
        setMetadataStatus('Р—Р°РіСЂСѓР¶Р°РµРј С„РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё...', 'pending');
        setBootstrapStatus('Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· Р·Р°РїСѓСЃС‚РёС‚СЃСЏ СЃСЂР°Р·Сѓ РїРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё С„РёР»СЊС‚СЂРѕРІ Рё РїСЂРёР·РЅР°РєРѕРІ.', 'pending');
        setDecisionSupportStatus('', '');

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
        renderForecastJobRuntime(null);
        if (button) {
            button.disabled = true;
        }

        try {
            await fetchForecastMetadata(baseQuery, requestToken);
            if (requestToken !== forecastRequestToken) {
                return;
            }

            setForecastLoadingState(
                'РЎРѕР±РёСЂР°РµРј Р±Р°Р·РѕРІС‹Р№ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·',
                'Р¤РёР»СЊС‚СЂС‹ СѓР¶Рµ РіРѕС‚РѕРІС‹. Р—Р°РіСЂСѓР¶Р°РµРј РґР°РЅРЅС‹Рµ, Р°РіСЂРµРіРёСЂСѓРµРј РёСЃС‚РѕСЂРёСЋ Рё РїРѕРґРіРѕС‚Р°РІР»РёРІР°РµРј Р±Р°Р·РѕРІС‹Р№ СЂР°СЃС‡С‘С‚.',
                1,
                { showSkeleton: true }
            );
            startBaseForecastProgressSequence();
            setMetadataStatus('Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹.', 'ready');
            setBootstrapStatus('РЎРѕР±РёСЂР°РµРј Р±Р°Р·РѕРІС‹Р№ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·...', 'pending');

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
                'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р±Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР·. РџРѕРїСЂРѕР±СѓР№С‚Рµ РёР·РјРµРЅРёС‚СЊ С„РёР»СЊС‚СЂС‹ РёР»Рё Р·Р°РїСѓСЃС‚РёС‚СЊ СЂР°СЃС‡С‘С‚ РµС‰Рµ СЂР°Р·.'
            );
            console.error(error);
            clearForecastStepTimers();
            if (error && error.forecastingStage === 'metadata') {
                setMetadataStatus(forecastErrorMessage, 'error');
                setBootstrapStatus('Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· РЅРµ Р·Р°РїСѓС‰РµРЅ: С„РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РЅРµ Р·Р°РіСЂСѓР·РёР»РёСЃСЊ.', 'error');
                setForecastProgress(0, {
                    lead: 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ С„РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё',
                    message: forecastErrorMessage,
                    isError: true
                });
                setDecisionSupportStatus('', '');
                showForecastError(forecastErrorMessage);
                return;
            }
            setMetadataStatus('Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹.', 'ready');
            setBootstrapStatus(forecastErrorMessage, 'error');
            setForecastProgress(2, {
                lead: 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РІРµСЂС€РёС‚СЊ Р±Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР·',
                message: forecastErrorMessage,
                isError: true
            });
            setDecisionSupportStatus('', '');
            showForecastError(forecastErrorMessage);
        } finally {
            if (button && requestToken === forecastRequestToken) {
                button.disabled = false;
            }
        }
    }
