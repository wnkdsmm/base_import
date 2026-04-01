// Jobs and async state

    function clearProgressTimers() {
        while (progressTimers.length) {
            clearTimeout(progressTimers.pop());
        }
    }

    function updateProgressStep(activeIndex, options) {
        var settings = options || {};
        var stepsContainer = byId('mlProgressSteps');
        var leadNode = byId('mlLoadingLead');
        var messageNode = byId('mlLoadingMessage');
        var activeStep = progressSteps[Math.max(0, Math.min(progressSteps.length - 1, activeIndex))];
        var isFinished = !!settings.isFinished;
        var isError = !!settings.isError;
        var leadText = settings.lead || activeStep.lead;
        var messageText = settings.message || activeStep.message;

        if (leadNode) {
            leadNode.textContent = leadText;
        }
        if (messageNode) {
            messageNode.textContent = messageText;
        }
        if (!stepsContainer) {
            return;
        }

        Array.prototype.forEach.call(stepsContainer.querySelectorAll('.ml-progress-step'), function (node, index) {
            node.classList.remove('is-active', 'is-done', 'is-error');
            if (isError && index === activeIndex) {
                node.classList.add('is-error');
                return;
            }
            if (isFinished) {
                if (index <= activeIndex) {
                    node.classList.add('is-done');
                }
                return;
            }
            if (index < activeIndex) {
                node.classList.add('is-done');
                return;
            }
            if (index === activeIndex) {
                node.classList.add('is-active');
            }
        });
    }

    function startProgressSequence() {
        clearProgressTimers();
        updateProgressStep(0);
        progressTimers.push(setTimeout(function () { updateProgressStep(1); }, 350));
        progressTimers.push(setTimeout(function () { updateProgressStep(2); }, 1100));
        progressTimers.push(setTimeout(function () { updateProgressStep(3); }, 1800));
    }

    function setRefreshButtonState(isBusy) {
        var button = byId('mlRefreshButton');
        if (!button) {
            return;
        }
        button.disabled = !!isBusy;
        button.classList.toggle('is-loading', !!isBusy);
    }

    function setLoadingStateMode(mode) {
        var loadingState = byId('mlLoadingState');
        var skeleton = byId('mlLoadingSkeleton');
        if (!loadingState) {
            return;
        }
        loadingState.classList.remove('is-pending', 'is-ready');
        if (mode === 'ready') {
            loadingState.classList.add('is-ready');
        } else {
            loadingState.classList.add('is-pending');
        }
        if (skeleton) {
            skeleton.classList.toggle('is-hidden', mode === 'ready');
        }
    }

    function showLoadingState() {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        if (asyncState) {
            asyncState.classList.remove('is-hidden');
        }
        if (loadingState) {
            loadingState.classList.remove('is-hidden');
        }
        if (errorState) {
            errorState.classList.add('is-hidden');
        }
        setText('mlErrorMessage', '');
        setLoadingStateMode('pending');
    }

    function hideLoadingState() {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        if (loadingState) {
            loadingState.classList.add('is-hidden');
        }
        if (asyncState && errorState && errorState.classList.contains('is-hidden')) {
            asyncState.classList.add('is-hidden');
        }
    }

    function showError(message) {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        var activeIndex = 0;
        if (asyncState) {
            asyncState.classList.remove('is-hidden');
        }
        if (loadingState) {
            loadingState.classList.remove('is-hidden');
        }
        if (errorState) {
            errorState.classList.remove('is-hidden');
        }
        if (currentJobState && currentJobState.status === 'running') {
            activeIndex = 1;
        }
        if (currentJobState && currentJobState.backtest_job
            && (currentJobState.backtest_job.status === 'running' || currentJobState.backtest_job.status === 'completed')) {
            activeIndex = 2;
        }
        setLoadingStateMode('ready');
        updateProgressStep(activeIndex, {
            isError: true,
            lead: 'Не удалось завершить ML-анализ',
            message: message || 'Попробуйте повторить запуск с теми же фильтрами.'
        });
        setText('mlErrorMessage', message || 'Не удалось загрузить ML-данные. Попробуйте еще раз.');
    }

    function hideError() {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        if (errorState) {
            errorState.classList.add('is-hidden');
        }
        setText('mlErrorMessage', '');
        if (asyncState && loadingState && loadingState.classList.contains('is-hidden')) {
            asyncState.classList.add('is-hidden');
        }
    }


    function stopJobPolling() {
        if (jobPollTimer) {
            clearTimeout(jobPollTimer);
            jobPollTimer = null;
        }
    }

    function updateAsyncStateForJob(jobPayload) {
        var safeJob = jobPayload || {};
        var backtestJob = safeJob.backtest_job || null;
        var activeIndex = 0;
        var lead = 'ML-задача поставлена в очередь';
        var message = 'Ожидаем запуска фонового расчёта.';
        var finished = false;

        if (safeJob.status === 'running') {
            activeIndex = 1;
            lead = 'Агрегируем историю и признаки';
            message = 'Собираем SQL-агрегаты, фильтры и дневной ряд для ML-прогноза.';
        }
        if (backtestJob && (backtestJob.status === 'running' || backtestJob.status === 'completed')) {
            activeIndex = 2;
            lead = backtestJob.status === 'completed' ? 'Валидация завершена' : 'Выполняем обучение и валидацию';
            message = backtestJob.logs && backtestJob.logs.length
                ? backtestJob.logs[backtestJob.logs.length - 1]
                : 'Проверяем модели на истории и выбираем рабочую конфигурацию.';
        }
        if (safeJob.logs && safeJob.logs.length) {
            message = safeJob.logs[safeJob.logs.length - 1];
        }
        if (safeJob.status === 'completed') {
            activeIndex = 3;
            lead = 'ML-анализ завершён';
            message = 'Результат готов, визуализации и таблицы уже подставлены в интерфейс.';
            finished = true;
        }
        setLoadingStateMode(finished ? 'ready' : 'pending');
        updateProgressStep(activeIndex, {
            isFinished: finished,
            lead: lead,
            message: message
        });
    }

    async function pollMlJob(jobId) {
        var response;
        var payload = null;

        if (!jobId) {
            return;
        }

        try {
            response = await fetch('/api/ml-model-jobs/' + encodeURIComponent(jobId), {
                headers: { Accept: 'application/json' }
            });
            payload = await response.json();
            currentJobState = payload;
            updateAsyncStateForJob(payload);

            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Фоновая ML-задача завершилась с ошибкой.');
            }

            if (payload.status === 'completed' && payload.result) {
                applyMlModelData(payload.result);
                hideError();
                isFetching = false;
                setRefreshButtonState(false);
                renderSidebarStatus(currentMlData || payload.result || window.__FIRE_ML_INITIAL__ || {});
                return;
            }

            jobPollTimer = setTimeout(function () {
                pollMlJob(jobId);
            }, 1200);
        } catch (error) {
            console.error(error);
            isFetching = false;
            setRefreshButtonState(false);
            hideLoadingState();
            showError(error && error.message ? error.message : 'Не удалось получить статус ML-задачи.');
            renderSidebarStatus(currentMlData || window.__FIRE_ML_INITIAL__ || {});
        }
    }

    async function startMlModelJob(options) {
        var settings = options || {};
        var requestPayload = buildRequestPayload(settings);
        var response;
        var payload = null;

        stopJobPolling();
        isFetching = true;
        currentJobState = null;
        setRefreshButtonState(true);
        showLoadingState();
        hideError();
        updateProgressStep(0, {
            lead: 'ML-задача поставлена в очередь',
            message: 'Подготавливаем фоновый запуск анализа.'
        });
        renderSidebarStatus(currentMlData || window.__FIRE_ML_INITIAL__ || {});

        if (settings.initialLoad) {
            showInitialSkeletons();
        }

        try {
            response = await fetch('/api/ml-model-jobs', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(requestPayload.body)
            });
            payload = await response.json();
            currentJobState = payload;
            updateAsyncStateForJob(payload);
            window.history.replaceState({}, '', requestPayload.query ? (window.location.pathname + '?' + requestPayload.query) : window.location.pathname);

            if (!response.ok || payload.status === 'failed' || payload.status === 'missing') {
                throw new Error(payload && payload.error_message ? payload.error_message : 'Не удалось запустить ML-задачу.');
            }

            if (payload.status === 'completed' && payload.result) {
                applyMlModelData(payload.result);
                updateAsyncStateForJob(payload);
                hideError();
                isFetching = false;
                setRefreshButtonState(false);
                renderSidebarStatus(currentMlData || payload.result || window.__FIRE_ML_INITIAL__ || {});
                return;
            }

            pollMlJob(payload.job_id);
        } catch (error) {
            console.error(error);
            isFetching = false;
            setRefreshButtonState(false);
            hideLoadingState();
            showError(error && error.message ? error.message : 'Не удалось запустить ML-анализ. Попробуйте еще раз.');
            renderSidebarStatus(currentMlData || window.__FIRE_ML_INITIAL__ || {});
        }
    }
