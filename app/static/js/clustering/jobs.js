// API and jobs

    function getClusteringApiErrorMessage(payload, fallback) {
        var normalizedFallback = fallback || 'Clustering request failed.';
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

    function createClusteringApiError(response, payload, fallback) {
        var error = new Error(getClusteringApiErrorMessage(payload, fallback));
        error.status = response && typeof response.status === 'number' ? response.status : 0;
        error.payload = payload || null;
        return error;
    }

    function getClusteringErrorMessage(error, fallback) {
        var message = error && typeof error.message === 'string' ? error.message.trim() : '';
        return message || fallback;
    }

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
        if (clusteringJobPollTimer) {
            clearTimeout(clusteringJobPollTimer);
            clusteringJobPollTimer = null;
        }
    }

    function renderClusteringJobRuntime(jobPayload) {
        var runtimeNode = byId('clusteringJobRuntime');
        var statusNode = byId('clusteringJobStatusLabel');
        var metaNode = byId('clusteringJobMeta');
        var logsNode = byId('clusteringJobLogOutput');
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
        statusNode.textContent = 'Статус clustering-job: ' + String(safeJob.status || 'pending');
        metaParts.push('job_id: ' + String(safeJob.job_id || ''));
        if (meta.cache_hit) {
            metaParts.push('кэш');
        }
        if (safeJob.reused) {
            metaParts.push('переиспользован');
        }
        metaNode.textContent = metaParts.join(' | ');
        logsNode.textContent = logs.length ? logs.join('\n') : 'Логи появятся после запуска фоновой задачи.';
    }

    function updateClusteringAsyncStateForJob(jobPayload) {
        var safeJob = jobPayload || {};
        var meta = safeJob.meta || {};
        var logs = Array.isArray(safeJob.logs) ? safeJob.logs : [];
        var activeIndex = Number(meta.stage_index);
        var lead = meta.stage_label ? 'Clustering: ' + String(meta.stage_label) : 'Фоновая clustering-задача';
        var message = String(meta.stage_message || '').trim();

        if (!Number.isFinite(activeIndex)) {
            activeIndex = 0;
        }
        if (!message && logs.length) {
            message = logs[logs.length - 1];
        }
        if (!message) {
            message = safeJob.status === 'pending'
                ? 'Ожидаем запуска фонового расчета.'
                : 'Кластеризация выполняется в фоне.';
        }

        if (safeJob.status === 'completed') {
            clearClusteringStepTimers();
            setClusteringReadyState(
                'Кластеры обновлены',
                'Агрегаты, кластеры и визуализации уже синхронизированы с текущими фильтрами.'
            );
            renderClusteringJobRuntime(safeJob);
            return;
        }

        setClusteringLoadingState(lead, message, activeIndex, {
            showSkeleton: true,
            stageMessage: message
        });
        setClusteringProgress(activeIndex, {
            lead: lead,
            message: message
        });
        renderClusteringJobRuntime(safeJob);
    }

    async function pollClusteringJob(jobId, query) {
        var response;
        var payload = null;
        if (!jobId) {
            return;
        }

        try {
            response = await fetch('/api/clustering-jobs/' + encodeURIComponent(jobId), {
                headers: { Accept: 'application/json' }
            });
            payload = await response.json();
            updateClusteringAsyncStateForJob(payload);

            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Фоновая clustering-задача завершилась с ошибкой.');
            }
            if (payload.status === 'completed' && payload.result) {
                applyClusteringData(payload.result);
                window.history.replaceState({}, '', query ? '/clustering?' + query : '/clustering');
                return;
            }

            clusteringJobPollTimer = setTimeout(function () {
                pollClusteringJob(jobId, query);
            }, 1200);
        } catch (error) {
            console.error(error);
            clearClusteringStepTimers();
            showClusteringError(getClusteringErrorMessage(
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
        renderClusteringJobRuntime(null);
        setClusteringLoadingState(
            'Пересчитываем кластеры',
            'Загружаем данные, агрегируем признаки и подготавливаем новые визуализации.',
            0,
            {
                showSkeleton: true,
                stageMessage: 'Расчёт кластеризации запущен для текущих фильтров.'
            }
        );
        startClusteringProgressSequence();

        try {
            var response = await fetch('/api/clustering-jobs', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            });
            var data = null;
            try {
                data = await response.json();
            } catch (error) {
                if (!response.ok) {
                    throw createClusteringApiError(response, null, 'От clustering API пришел не JSON-ответ.');
                }
                throw error;
            }
            if (!response.ok) {
                throw createClusteringApiError(response, data, 'Не удалось выполнить запрос clustering.');
            }
            if (data && data.ok === false) {
                throw createClusteringApiError(response, data, 'Не удалось выполнить запрос clustering.');
            }
            if (data && data.bootstrap_mode === 'deferred' && !data.has_data) {
                throw new Error('В clustering API вернулся shell-пейлоад вместо готовых данных.');
            }
            updateClusteringAsyncStateForJob(data);
            if (data.status === 'completed' && data.result) {
                applyClusteringData(data.result);
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
            clearClusteringStepTimers();
            setClusteringProgress(2, {
                lead: 'Не удалось пересчитать кластеры',
                message: clusteringErrorMessage,
                isError: true
            });
            showClusteringError(clusteringErrorMessage);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }
