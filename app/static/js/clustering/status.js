// Async state

    function clearClusteringStepTimers() {
        while (clusteringStepTimers.length) {
            clearTimeout(clusteringStepTimers.pop());
        }
    }

    function setClusteringAsyncVisibility(visible) {
        var asyncNode = byId('clusteringAsyncState');
        if (!asyncNode) {
            return;
        }
        asyncNode.classList.toggle('is-hidden', !visible);
    }

    function setClusteringSkeletonVisible(visible) {
        var skeletonNode = byId('clusteringLoadingSkeleton');
        if (!skeletonNode) {
            return;
        }
        skeletonNode.classList.toggle('is-hidden', !visible);
    }

    function hideClusteringError() {
        var errorNode = byId('clusteringErrorState');
        if (!errorNode) {
            return;
        }
        errorNode.classList.add('is-hidden');
        setText('clusteringErrorMessage', '');
    }

    function showClusteringError(message) {
        var loadingNode = byId('clusteringLoadingState');
        var errorNode = byId('clusteringErrorState');
        setClusteringAsyncVisibility(true);
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        setClusteringSkeletonVisible(false);
        setText('clusteringErrorMessage', message || 'Не удалось пересчитать кластеры. Попробуйте еще раз.');
        if (errorNode) {
            errorNode.classList.remove('is-hidden');
        }
    }

    function setClusteringProgress(activeIndex, options) {
        var stepsNode = byId('clusteringProgressSteps');
        var lead = options && options.lead ? options.lead : '';
        var message = options && options.message ? options.message : '';
        var isFinished = Boolean(options && options.isFinished);
        var isError = Boolean(options && options.isError);

        setText('clusteringLoadingLead', lead);
        setText('clusteringLoadingMessage', message);

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

    function setClusteringLoadingState(lead, message, activeIndex, options) {
        var loadingNode = byId('clusteringLoadingState');
        var stageNode = byId('clusteringStageStatus');
        var settings = options || {};
        setClusteringAsyncVisibility(true);
        hideClusteringError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-ready');
            loadingNode.classList.add('is-pending');
        }
        if (stageNode) {
            stageNode.textContent = settings.stageMessage || message;
            stageNode.classList.remove('is-hidden', 'is-ready', 'is-error');
            stageNode.classList.add('is-pending');
        }
        setClusteringSkeletonVisible(settings.showSkeleton !== false);
        setClusteringProgress(activeIndex, {
            lead: lead,
            message: message
        });
    }

    function setClusteringReadyState(lead, message) {
        var loadingNode = byId('clusteringLoadingState');
        var stageNode = byId('clusteringStageStatus');
        setClusteringAsyncVisibility(true);
        hideClusteringError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        if (stageNode) {
            stageNode.textContent = message;
            stageNode.classList.remove('is-hidden', 'is-pending', 'is-error');
            stageNode.classList.add('is-ready');
        }
        setClusteringSkeletonVisible(false);
        setClusteringProgress(3, {
            lead: lead,
            message: message,
            isFinished: true
        });
    }

    function startClusteringProgressSequence() {
        clearClusteringStepTimers();
        setClusteringProgress(0, {
            lead: 'Загружаем исходные данные',
            message: 'Получаем территориальные записи и выбранные параметры кластеризации.'
        });
        clusteringStepTimers.push(setTimeout(function () {
            setClusteringProgress(1, {
                lead: 'Агрегируем территориальные признаки',
                message: 'Собираем агрегаты по территориям и проверяем заполненность признаков.'
            });
        }, 320));
        clusteringStepTimers.push(setTimeout(function () {
            setClusteringProgress(2, {
                lead: 'Считаем кластеры и диагностики',
                message: 'Запускаем сегментацию, quality-метрики и профили кластеров.'
            });
        }, 980));
        clusteringStepTimers.push(setTimeout(function () {
            setClusteringProgress(3, {
                lead: 'Подготавливаем визуализации',
                message: 'Собираем scatter, распределения и итоговые таблицы.'
            });
        }, 1700));
    }

    function syncClusteringAsyncState(data) {
        if (!data) {
            setClusteringAsyncVisibility(false);
            return;
        }

        if (data.bootstrap_mode === 'deferred' && !data.has_data) {
            setClusteringLoadingState(
                'Подготавливаем типологию территорий',
                'Открыт лёгкий shell страницы. Догружаем расчёт кластеризации в фоне.',
                0,
                {
                    showSkeleton: true,
                    stageMessage: 'Открыт лёгкий shell страницы. Догружаем расчёт кластеризации в фоне.'
                }
            );
            return;
        }

        if (data.has_data) {
            clearClusteringStepTimers();
            setClusteringReadyState(
                'Кластеры обновлены',
                'Агрегаты, кластеры и визуализации уже синхронизированы с текущими фильтрами.'
            );
            return;
        }

        setClusteringAsyncVisibility(false);
    }

