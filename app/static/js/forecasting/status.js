// Async state and progress

    function clearForecastStepTimers() {
        while (forecastStepTimers.length) {
            clearTimeout(forecastStepTimers.pop());
        }
    }

    function setForecastAsyncVisibility(visible) {
        var asyncNode = byId('forecastAsyncState');
        if (!asyncNode) {
            return;
        }
        asyncNode.classList.toggle('is-hidden', !visible);
    }

    function setForecastSkeletonVisible(visible) {
        var skeletonNode = byId('forecastLoadingSkeleton');
        if (!skeletonNode) {
            return;
        }
        skeletonNode.classList.toggle('is-hidden', !visible);
    }

    function hideForecastError() {
        var errorNode = byId('forecastErrorState');
        if (!errorNode) {
            return;
        }
        errorNode.classList.add('is-hidden');
        setText('forecastErrorMessage', '');
    }

    function showForecastError(message) {
        var loadingNode = byId('forecastLoadingState');
        var errorNode = byId('forecastErrorState');
        setForecastAsyncVisibility(true);
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        setForecastSkeletonVisible(false);
        setText('forecastErrorMessage', message || 'Не удалось пересчитать прогноз. Попробуйте еще раз.');
        if (errorNode) {
            errorNode.classList.remove('is-hidden');
        }
    }

    function setForecastProgress(activeIndex, options) {
        var stepsNode = byId('forecastProgressSteps');
        var lead = options && options.lead ? options.lead : '';
        var message = options && options.message ? options.message : '';
        var isFinished = Boolean(options && options.isFinished);
        var isError = Boolean(options && options.isError);

        setText('forecastLoadingLead', lead);
        setText('forecastLoadingMessage', message);

        if (!stepsNode) {
            return;
        }

        Array.prototype.forEach.call(stepsNode.querySelectorAll('.analysis-step'), function (node, index) {
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

    function setForecastLoadingState(lead, message, activeIndex, options) {
        var loadingNode = byId('forecastLoadingState');
        var settings = options || {};
        setForecastAsyncVisibility(true);
        hideForecastError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-ready');
            loadingNode.classList.add('is-pending');
        }
        setForecastSkeletonVisible(settings.showSkeleton !== false);
        setForecastProgress(activeIndex, {
            lead: lead,
            message: message
        });
    }

    function setForecastReadyState(lead, message) {
        var loadingNode = byId('forecastLoadingState');
        setForecastAsyncVisibility(true);
        hideForecastError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        setForecastSkeletonVisible(false);
        setForecastProgress(3, {
            lead: lead,
            message: message,
            isFinished: true
        });
    }

    function startForecastProgressSequence() {
        clearForecastStepTimers();
        setForecastProgress(0, {
            lead: 'Загружаем фильтры и признаки',
            message: 'Подготавливаем данные для формы: доступные значения фильтров и найденные группы признаков.'
        });
        forecastStepTimers.push(setTimeout(function () {
            setForecastProgress(1, {
                lead: 'Подготавливаем базовый прогноз',
                message: 'Фильтры уже готовы. Переходим к сбору истории и базового сценарного прогноза.'
            });
        }, 320));
    }

    function startBaseForecastProgressSequence() {
        clearForecastStepTimers();
        setForecastProgress(1, {
            lead: 'Агрегируем историю пожаров',
            message: 'Собираем дневной ряд и ключевые показатели по выбранным фильтрам.'
        });
        forecastStepTimers.push(setTimeout(function () {
            setForecastProgress(2, {
                lead: 'Считаем базовый прогноз и проверку',
                message: 'Обновляем базовый сценарий и метрики по историческому ряду.'
            });
        }, 640));
    }

    function syncForecastAsyncState(data) {
        if (!data) {
            setForecastAsyncVisibility(false);
            return;
        }

        if (data.metadata_pending) {
            clearForecastStepTimers();
            setForecastLoadingState(
                'Подготавливаем фильтры и признаки',
                data.metadata_status_message || 'Сначала догружаем фильтры и признаки для страницы прогноза.',
                0,
                { showSkeleton: true }
            );
            return;
        }

        if (data.decision_support_error) {
            clearForecastStepTimers();
            setForecastReadyState(
                'Базовый прогноз готов, блок поддержки решений требует повтора',
                data.decision_support_status_message || 'Базовый прогноз уже показан, но догрузка поддержки решений завершилась ошибкой.'
            );
            showForecastError(data.decision_support_status_message || 'Не удалось догрузить блок поддержки решений. Попробуйте еще раз.');
            return;
        }

        if (data.decision_support_pending) {
            clearForecastStepTimers();
            setForecastLoadingState(
                'Базовый прогноз готов, догружаем поддержку решений',
                data.decision_support_status_message || 'Подтягиваем приоритеты территорий, рекомендации и финальные визуализации.',
                3,
                { showSkeleton: false }
            );
            return;
        }

        if (data.loading || data.bootstrap_mode === 'deferred') {
            setForecastLoadingState(
                data.metadata_ready ? 'Собираем базовый сценарный прогноз' : 'Подготавливаем сценарный прогноз',
                data.loading_status_message || 'После загрузки фильтров и признаков рассчитываем базовый прогноз, а затем догружаем поддержку решений.',
                data.metadata_ready ? 1 : 0,
                { showSkeleton: true }
            );
            return;
        }

        if (data.base_forecast_ready || data.decision_support_ready) {
            clearForecastStepTimers();
            setForecastReadyState(
                'Сценарный прогноз обновлён',
                data.decision_support_ready
                    ? 'Базовый прогноз, визуализации и блок поддержки решений уже синхронизированы.'
                    : (data.loading_status_message || 'Базовый прогноз и визуализации готовы.')
            );
            return;
        }

        setForecastAsyncVisibility(false);
    }

    function buildForecastRequestQuery(baseQuery, includeDecisionSupport) {
        var params = new URLSearchParams(baseQuery || '');
        params.set('include_decision_support', includeDecisionSupport ? '1' : '0');
        return params.toString();
    }

    function setMetadataStatus(message, state) {
        var node = byId('forecastMetadataStatus');
        var text = String(message == null ? '' : message).trim();
        if (!node) {
            return;
        }

        node.textContent = text;
        node.classList.toggle('is-hidden', !text);
        node.classList.remove('is-pending', 'is-ready', 'is-error');
        if (text && state) {
            node.classList.add('is-' + state);
        }
    }

    function syncMetadataStatus(data) {
        if (!data) {
            setMetadataStatus('', '');
            return;
        }

        if (data.metadata_error) {
            setMetadataStatus(data.metadata_status_message, 'error');
            return;
        }
        if (data.metadata_pending) {
            setMetadataStatus(data.metadata_status_message, 'pending');
            return;
        }
        if (data.metadata_ready && data.metadata_status_message) {
            setMetadataStatus(data.metadata_status_message, 'ready');
            return;
        }
        setMetadataStatus('', '');
    }

    function setBootstrapStatus(message, state) {
        var node = byId('forecastBootstrapStatus');
        var text = String(message == null ? '' : message).trim();
        if (!node) {
            return;
        }

        node.textContent = text;
        node.classList.toggle('is-hidden', !text);
        node.classList.remove('is-pending', 'is-ready', 'is-error');
        if (text && state) {
            node.classList.add('is-' + state);
        }
    }

    function syncBootstrapStatus(data) {
        if (!data) {
            setBootstrapStatus('', '');
            return;
        }

        if (data.loading && data.loading_status_message) {
            setBootstrapStatus(data.loading_status_message, 'pending');
            return;
        }
        if (data.base_forecast_ready && data.loading_status_message) {
            setBootstrapStatus(data.loading_status_message, 'ready');
            return;
        }
        setBootstrapStatus('', '');
    }

    function setDecisionSupportStatus(message, state) {
        var node = byId('forecastDecisionSupportStatus');
        var text = String(message == null ? '' : message).trim();
        if (!node) {
            return;
        }

        node.textContent = text;
        node.classList.toggle('is-hidden', !text);
        node.classList.remove('is-pending', 'is-ready', 'is-error');
        if (text && state) {
            node.classList.add('is-' + state);
        }
    }

    function syncDecisionSupportStatus(data) {
        if (!data) {
            setDecisionSupportStatus('', '');
            return;
        }

        if (data.decision_support_error) {
            setDecisionSupportStatus(data.decision_support_status_message, 'error');
            return;
        }
        if (data.decision_support_pending) {
            setDecisionSupportStatus(data.decision_support_status_message, 'pending');
            return;
        }
        if (data.decision_support_ready && data.decision_support_status_message) {
            setDecisionSupportStatus(data.decision_support_status_message, 'ready');
            return;
        }
        setDecisionSupportStatus('', '');
    }

    function syncSidebarBadge(data) {
        var node = document.querySelector('.sidebar-status .status-badge');
        if (!node) {
            return;
        }

        if (data && data.bootstrap_mode === 'deferred') {
            node.textContent = 'Подготавливаем прогноз';
            node.classList.add('status-badge-live');
            return;
        }
        if (data && data.has_data) {
            node.textContent = 'Сценарный прогноз собран';
            node.classList.add('status-badge-live');
            return;
        }
        node.textContent = 'Нужно уточнить фильтры';
        node.classList.remove('status-badge-live');
    }

