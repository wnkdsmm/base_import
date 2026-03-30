(function () {
    function byId(id) {
        return document.getElementById(id);
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function setText(id, value) {
        var node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
    }

    function setSelectOptions(id, options, selectedValue, emptyLabel) {
        var selectNode = byId(id);
        if (!selectNode) {
            return;
        }

        var safeOptions = Array.isArray(options) && options.length ? options : [{ value: '', label: emptyLabel }];
        selectNode.innerHTML = safeOptions.map(function (option) {
            var selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        }).join('');
    }

    var clusteringStepTimers = [];
    var clusteringJobPollTimer = null;

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

    function renderChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var figure = chart && chart.plotly;
        if (!window.Plotly || !figure || !Array.isArray(figure.data) || !figure.data.length) {
            chartNode.innerHTML = '';
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            return;
        }

        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
    }

    function renderSidebarStatus(data) {
        var container = byId('clusteringSidebarStatus');
        if (!container) {
            return;
        }

        var summary = data.summary || {};
        var isLoaded = Boolean(data && data.has_data);
        var isDeferred = data && data.bootstrap_mode === 'deferred';
        var badgeClass = 'status-badge';
        if (isLoaded) {
            badgeClass += ' status-badge-live';
        }

        var badgeLabel = isDeferred
            ? 'Подготовка типологии'
            : (isLoaded ? 'Типы территорий рассчитаны' : 'Нужны агрегированные признаки');

        container.innerHTML = ''
            + '<span class="' + badgeClass + '">' + escapeHtml(badgeLabel) + '</span>'
            + '<div class="status-line"><span>Таблица</span><strong>' + escapeHtml(summary.selected_table_label || 'Нет таблицы') + '</strong></div>'
            + '<div class="status-line"><span>Территорий</span><strong>' + escapeHtml(summary.clustered_entities_display || '0') + '</strong></div>'
            + '<div class="status-line"><span>Силуэт</span><strong>' + escapeHtml(summary.silhouette_display || '—') + '</strong></div>';
    }

    function renderHero(data) {
        var summary = data.summary || {};
        setText(
            'clusteringModelDescription',
            data.model_description || 'Выберите таблицу, чтобы собрать агрегированный профиль территории и выделить типы риска.'
        );

        var heroTags = byId('clusteringHeroTags');
        if (heroTags) {
            heroTags.innerHTML = ''
                + '<span class="hero-tag">Таблица: <strong>' + escapeHtml(summary.selected_table_label || 'Нет таблицы') + '</strong></span>'
                + '<span class="hero-tag">Пожаров в истории: <strong>' + escapeHtml(summary.total_incidents_display || '0') + '</strong></span>'
                + '<span class="hero-tag">Территорий в модели: <strong>' + escapeHtml(summary.clustered_entities_display || '0') + '</strong></span>';
        }

        var heroStats = byId('clusteringHeroStats');
        if (heroStats) {
            heroStats.innerHTML = ''
                + '<article class="hero-stat-card">'
                + '<span class="hero-stat-label">Лучший k по силуэту</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.suggested_cluster_count_display || '—') + '</strong>'
                + '<span class="hero-stat-foot">Подсказка по числу кластеров на основе качества разделения.</span>'
                + '</article>'
                + '<article class="hero-stat-card hero-stat-card-soft">'
                + '<span class="hero-stat-label">Точка локтя</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.elbow_cluster_count_display || '—') + '</strong>'
                + '<span class="hero-stat-foot">Где кривая inertia начинает заметно ломаться.</span>'
                + '</article>';
        }
    }

    function renderFeaturePicker(filters) {
        var container = byId('clusteringFeaturePicker');
        if (!container) {
            return;
        }

        var items = Array.isArray(filters.available_features) ? filters.available_features : [];
        var selectedValues = Array.isArray(filters.feature_columns)
            ? filters.feature_columns.map(function (item) { return String(item); })
            : [];

        var body;
        if (items.length) {
            body = '<div class="cluster-feature-grid">' + items.map(function (feature) {
                var checked = feature.is_selected || selectedValues.indexOf(String(feature.name)) >= 0 ? ' checked' : '';
                return ''
                    + '<label class="cluster-feature-option">'
                    + '<input type="checkbox" name="feature_columns" value="' + escapeHtml(feature.name) + '"' + checked + '>'
                    + '<span class="cluster-feature-copy">'
                    + '<strong class="cluster-feature-name">' + escapeHtml(feature.name) + '</strong>'
                    + '<span class="cluster-feature-meta">' + escapeHtml(feature.description || '')
                    + ' Заполненность: ' + escapeHtml(feature.coverage_display || '0%')
                    + ' | Дисперсия: ' + escapeHtml(feature.variance_display || '0') + '</span>'
                    + '</span>'
                    + '</label>';
            }).join('') + '</div>';
        } else {
            body = '<div class="mini-empty">После выбора таблицы здесь появятся агрегированные признаки для типологии территорий риска.</div>';
        }

        container.innerHTML = ''
            + '<span>Агрегированные признаки территории</span>'
            + body
            + '<span class="cluster-feature-help">Базовый набор уже ориентирован на территориальный риск: частота, площадь, ночные пожары, прибытие, последствия и подтвержденность водоснабжения.</span>';
    }

    function renderFilterSummary(summary) {
        var container = byId('clusteringFilterSummary');
        if (!container) {
            return;
        }

        container.textContent = 'Пожаров в истории: ' + (summary.total_incidents_display || '0')
            + ' | Территорий после агрегации: ' + (summary.total_entities_display || '0')
            + ' | В выборке: ' + (summary.sampled_entities_display || '0')
            + ' | Стратегия: ' + (summary.sampling_strategy_label || 'Не выбрана');
    }

    function renderSummaryCards(summary) {
        var container = byId('clusteringStatsGrid');
        if (!container) {
            return;
        }

        container.innerHTML = ''
            + '<article class="stat-card stat-card-accent">'
            + '<span class="stat-label">Пожаров в истории</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.total_incidents_display || '0') + '</strong>'
            + '<span class="stat-foot">Все инциденты, которые вошли в территориальные агрегаты.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Территорий в расчете</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.clustered_entities_display || '0') + '</strong>'
            + '<span class="stat-foot">После отбора по заполненности выбранных признаков.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Число кластеров</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.cluster_count_display || '0') + '</strong>'
            + '<span class="stat-foot">Запрошено: ' + escapeHtml(summary.cluster_count_requested_display || '0') + '</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Инерция</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.inertia_display || '0') + '</strong>'
            + '<span class="stat-foot">Внутрикластерная компактность после стандартизации агрегатов.</span>'
            + '</article>';
    }

    function renderMetricCards(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return ''
                + '<article class="stat-card">'
                + '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>'
                + '<strong class="stat-value">' + escapeHtml(item.value || '-') + '</strong>'
                + '<span class="stat-foot">' + escapeHtml(item.meta || '') + '</span>'
                + '</article>';
        }).join('');
    }

    function renderQualityTable(rows) {
        var container = byId('clusteringQualityTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">Сравнение алгоритмов появится после расчета.</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="data-table table-stack-mobile">'
            + '<thead><tr><th>Метод</th><th>Силуэт</th><th>Индекс Дэвиса-Болдина</th><th>Индекс Калински-Харабаза</th><th>Баланс</th><th>Статус</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Метод">' + escapeHtml(row.method_label || '-') + '</td>'
                    + '<td data-label="Силуэт">' + escapeHtml(row.silhouette_display || '-') + '</td>'
                    + '<td data-label="Индекс Дэвиса-Болдина">' + escapeHtml(row.davies_display || '-') + '</td>'
                    + '<td data-label="Индекс Калински-Харабаза">' + escapeHtml(row.calinski_display || '-') + '</td>'
                    + '<td data-label="Баланс">' + escapeHtml(row.balance_display || '-') + '</td>'
                    + '<td data-label="Статус">' + escapeHtml(row.selection_label || '-') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderNoticeList(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<li>' + escapeHtml(emptyMessage) + '</li>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<li>' + escapeHtml(item) + '</li>';
        }).join('');
    }

    function renderDataTable(containerId, columns, rows, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(columns) || !columns.length || !Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="data-table table-sticky-first">'
            + '<thead><tr>' + columns.map(function (column) {
                return '<th>' + escapeHtml(column) + '</th>';
            }).join('') + '</tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                var cells = Array.isArray(row) ? row : [];
                return '<tr>' + cells.map(function (cell) {
                    return '<td>' + escapeHtml(cell) + '</td>';
                }).join('') + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderProfiles(items) {
        var container = byId('clusterProfilesShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Профили типов территорий появятся после расчета.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return ''
                + '<article class="cluster-overview-card tone-' + escapeHtml(item.tone || 'sky') + '">'
                + '<div class="cluster-overview-head">'
                + '<strong>' + escapeHtml(item.cluster_label || 'Кластер') + '</strong>'
                + '<span class="cluster-badge">' + escapeHtml(item.share_display || '0%') + '</span>'
                + '</div>'
                + '<span class="cluster-overview-meta">' + escapeHtml(item.segment_title || '') + '</span>'
                + '<span class="cluster-overview-meta">Территорий: <span class="cluster-overview-value">' + escapeHtml(item.size_display || '0') + '</span> | Пожаров в истории: <span class="cluster-overview-value">' + escapeHtml(item.incidents_display || '0') + '</span></span>'
                + '<span class="cluster-overview-foot">' + escapeHtml(item.summary || '') + '</span>'
                + '</article>';
        }).join('');
    }

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

    function applyClusteringData(data) {
        if (!data) {
            return;
        }

        var summary = data.summary || {};
        var filters = data.filters || {};
        var quality = data.quality_assessment || {};
        var charts = data.charts || {};

        renderSidebarStatus(data);
        renderHero(data);

        setSelectOptions('clusterTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('clusterCountFilter', filters.available_cluster_counts, filters.cluster_count, '4 кластера');
        setSelectOptions('clusterSampleLimitFilter', filters.available_sample_limits, filters.sample_limit, 'до 1000 территорий');
        setSelectOptions('clusterSamplingStrategyFilter', filters.available_sampling_strategies, filters.sampling_strategy, 'Стратифицированная');
        renderFeaturePicker(filters);
        renderFilterSummary(summary);
        renderSummaryCards(summary);

        setText('clusteringQualityTitle', quality.title || 'Оценка качества кластеризации');
        setText('clusteringQualitySubtitle', quality.subtitle || 'После расчета здесь появятся внутренние метрики качества и сравнение алгоритмов.');
        renderMetricCards('clusteringQualityMetrics', quality.metric_cards || [], 'После расчета здесь появятся внутренние метрики качества кластеризации.');
        renderMetricCards('clusteringQualityMethodology', quality.methodology_items || [], 'Методология сравнения появится после расчета.');
        renderQualityTable(quality.comparison_rows || []);
        renderNoticeList('clusteringQualityNotes', quality.dissertation_points || [], 'После расчета здесь появятся формулировки для раздела о качестве.');

        setText('clusterScatterTitle', charts.scatter ? charts.scatter.title : 'Кластеры территорий на двумерной проекции');
        setText('clusterDistributionTitle', charts.distribution ? charts.distribution.title : 'Размеры кластеров по числу территорий');
        setText('clusterDiagnosticsTitle', charts.diagnostics ? charts.diagnostics.title : 'Подсказка по числу кластеров');
        renderChart(charts.scatter, 'clusterScatterChart', 'clusterScatterChartFallback');
        renderChart(charts.distribution, 'clusterDistributionChart', 'clusterDistributionChartFallback');
        renderChart(charts.diagnostics, 'clusterDiagnosticsChart', 'clusterDiagnosticsChartFallback');

        renderDataTable('clusterCentroidTableShell', data.centroid_columns, data.centroid_rows, 'После расчета здесь появятся средние профили кластеров.');
        renderProfiles(data.cluster_profiles || []);
        renderNoticeList('clusterNotesList', data.notes || [], 'После расчета здесь появятся комментарии по качеству сегментации и смыслу полученных типов территорий.');
        renderDataTable('clusterRepresentativesTableShell', data.representative_columns, data.representative_rows, 'После расчета здесь появятся территории, ближайшие к центрам кластеров.');
        syncClusteringAsyncState(data);
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

    document.addEventListener('DOMContentLoaded', function () {
        var form = byId('clusteringForm');
        var initialData = window.__FIRE_CLUSTERING_INITIAL__;

        if (form) {
            var tableFilter = byId('clusterTableFilter');
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchClusteringData();
            });

            if (tableFilter) {
                tableFilter.addEventListener('change', function () {
                    Array.prototype.forEach.call(
                        form.querySelectorAll('input[name="feature_columns"]'),
                        function (field) {
                            field.checked = false;
                        }
                    );
                    fetchClusteringData();
                });
            }
        }
        var retryButton = byId('clusteringRetryButton');
        if (retryButton) {
            retryButton.addEventListener('click', function () {
                fetchClusteringData();
            });
        }

        if (initialData) {
            applyClusteringData(initialData);
        }
        if (!initialData || initialData.bootstrap_mode === 'deferred') {
            fetchClusteringData();
        }
    });
})();
