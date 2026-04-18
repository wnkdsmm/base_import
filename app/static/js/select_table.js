(function () {
    const shared = window.FireUi;
    const createJobId = shared.createJobId;
    const escapeHtml = shared.escapeHtml;
    const fetchJson = shared.fetchJson;
    const setStepProgress = shared.setStepProgress;
    let selectedTable = null;
    let isRunning = false;
    const profilingDefaults = (function () {
        const rawConfig = document.body ? document.body.getAttribute('data-profiling-defaults') : '';
        if (!rawConfig) {
            return {};
        }
        try {
            return JSON.parse(rawConfig);
        } catch (error) {
            return {};
        }
    }());

    function formatPercent(value) {
        if (value === null || value === undefined || Number.isNaN(Number(value))) return '0%';
        return `${(Number(value) * 100).toFixed(1)}%`;
    }

    function setStatus(type, message) {
        const banner = document.getElementById('statusBanner');
        if (!banner) return;
        banner.className = `analysis-runtime-card status-banner status-${type}`;
        banner.innerHTML = message;
    }

    const profilingStepTimers = shared.createTimerGroup();

    function clearProfilingStepTimers() {
        profilingStepTimers.clear();
    }

    function setProfilingSkeletonVisible(visible) {
        const skeleton = document.getElementById('profilingLoadingSkeleton');
        if (!skeleton) return;
        skeleton.classList.toggle('is-hidden', !visible);
    }

    function hideProfilingError() {
        const errorNode = document.getElementById('profilingErrorState');
        const messageNode = document.getElementById('profilingErrorMessage');
        if (errorNode) errorNode.classList.add('is-hidden');
        if (messageNode) messageNode.textContent = '';
    }

    function showProfilingError(message) {
        const errorNode = document.getElementById('profilingErrorState');
        const messageNode = document.getElementById('profilingErrorMessage');
        const loadingNode = document.getElementById('profilingLoadingState');
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        setProfilingSkeletonVisible(false);
        if (messageNode) {
            messageNode.textContent = message || 'Не удалось завершить очистку. Попробуйте еще раз.';
        }
        if (errorNode) {
            errorNode.classList.remove('is-hidden');
        }
    }

    function setProfilingProgress(activeIndex, options = {}) {
        setStepProgress({
            activeIndex,
            isError: options.isError,
            isFinished: options.isFinished,
            lead: options.lead,
            leadId: 'profilingLoadingLead',
            message: options.message,
            messageId: 'profilingLoadingMessage',
            stepSelector: '.analysis-step',
            stepsId: 'profilingProgressSteps'
        });
    }

    function setProfilingLoadingState(lead, message, activeIndex, options = {}) {
        const loadingNode = document.getElementById('profilingLoadingState');
        hideProfilingError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-ready');
            loadingNode.classList.add('is-pending');
        }
        setProfilingSkeletonVisible(options.showSkeleton !== false);
        setProfilingProgress(activeIndex, { lead, message });
    }

    function setProfilingReadyState(lead, message) {
        const loadingNode = document.getElementById('profilingLoadingState');
        hideProfilingError();
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-pending');
            loadingNode.classList.add('is-ready');
        }
        setProfilingSkeletonVisible(false);
        setProfilingProgress(3, { lead, message, isFinished: true });
    }

    function startProfilingProgressSequence() {
        clearProfilingStepTimers();
        setProfilingProgress(0, {
            lead: 'Загружаем таблицу и проверяем параметры',
            message: 'Получаем выбранную таблицу и пороги очистки.'
        });
        profilingStepTimers.set(() => {
            setProfilingProgress(1, {
                lead: 'Агрегируем метрики по колонкам',
                message: 'Считаем пропуски, доминирующие значения и дисперсию.'
            });
        }, 260);
        profilingStepTimers.set(() => {
            setProfilingProgress(2, {
                lead: 'Проверяем правила очистки',
                message: 'Формируем итоговый набор колонок и валидируем результат.'
            });
        }, 920);
        profilingStepTimers.set(() => {
            setProfilingProgress(3, {
                lead: 'Собираем итоговую сводку',
                message: 'Подготавливаем таблицу clean_* и файлы результата.'
            });
        }, 1650);
    }

    function renderRunSummary(summary = {}) {
        const tableEl = document.getElementById('runSummaryTable');
        const removedEl = document.getElementById('runSummaryRemoved');
        const cleanTableEl = document.getElementById('runSummaryCleanTable');
        if (!tableEl || !removedEl || !cleanTableEl) return;
        tableEl.textContent = summary.tableName || 'Не выбрана';
        removedEl.textContent = summary.removedCount ?? '-';
        cleanTableEl.textContent = summary.cleanTable || '-';
    }

    function readThresholds() {
        const nullValue = Number(document.getElementById('nullThreshold')?.value ?? profilingDefaults.null_threshold_percent);
        const dominantValue = Number(document.getElementById('dominantThreshold')?.value ?? profilingDefaults.dominant_value_threshold_percent);
        const varianceValue = Number(document.getElementById('varianceThreshold')?.value ?? profilingDefaults.low_variance_threshold);
        if (Number.isNaN(nullValue) || nullValue < 0 || nullValue > 100) throw new Error('Порог пропусков должен быть от 0 до 100%.');
        if (Number.isNaN(dominantValue) || dominantValue < 0 || dominantValue > 100) throw new Error('Порог доминирующего значения должен быть от 0 до 100%.');
        if (Number.isNaN(varianceValue) || varianceValue < 0) throw new Error('Порог дисперсии должен быть неотрицательным числом.');
        return {
            null_threshold: nullValue / 100,
            dominant_value_threshold: dominantValue / 100,
            low_variance_threshold: varianceValue,
        };
    }

    function selectTable(tableName) {
        const selectionNote = document.getElementById('selectionNote');
        const runButton = document.getElementById('runButton');
        if (!tableName) {
            selectedTable = null;
            clearProfilingStepTimers();
            hideProfilingError();
            document.getElementById('profilingLoadingState')?.classList.add('is-hidden');
            if (selectionNote) selectionNote.textContent = 'Таблица пока не выбрана.';
            if (runButton) {
                runButton.disabled = true;
                runButton.classList.remove('is-loading');
            }
            renderRunSummary({ tableName: 'Не выбрана', removedCount: '-', cleanTable: '-' });
            setStatus('idle', 'Выберите таблицу, при необходимости измените пороги и запустите очистку.');
            return;
        }

        selectedTable = tableName;
        hideProfilingError();
        document.getElementById('profilingLoadingState')?.classList.add('is-hidden');
        if (selectionNote) selectionNote.textContent = `Выбрана таблица: ${tableName}`;
        if (runButton) {
            runButton.disabled = false;
            runButton.classList.remove('is-loading');
        }
        renderRunSummary({ tableName, removedCount: '-', cleanTable: `clean_${tableName}` });
        setStatus('idle', `Выбрана таблица <strong>${escapeHtml(tableName)}</strong>. Можно запускать очистку с текущими порогами.`);
    }

    function renderThresholds(thresholds) {
        const container = document.getElementById('thresholdsSummary');
        if (!container || !thresholds) return;
        container.innerHTML = [
            `Порог пропусков: ${Math.round((thresholds.null_threshold ?? 0) * 100)}%`,
            `Порог доминирующего значения: ${Math.round((thresholds.dominant_value_threshold ?? 0) * 100)}%`,
            `Минимальная дисперсия: ${thresholds.low_variance_threshold ?? 0}`,
        ].map((text) => `<div class="chip">${escapeHtml(text)}</div>`).join('');
    }

    function renderReasonSummary(reasonSummary) {
        const container = document.getElementById('reasonSummary');
        if (!container) return;
        if (!Array.isArray(reasonSummary) || !reasonSummary.length) {
            container.innerHTML = '<div class="reason-item"><div class="reason-title">Нет данных</div><div class="reason-meta">Сводка по причинам исключения пока недоступна.</div></div>';
            return;
        }
        container.innerHTML = reasonSummary.filter((item) => Number(item.count) > 0).map((item) => `<div class="reason-item"><div class="reason-title">${escapeHtml(item.label)}: ${escapeHtml(item.count)}</div><div class="reason-meta">${escapeHtml(item.description)}</div></div>`).join('') || '<div class="reason-item"><div class="reason-title">Ничего не исключено</div><div class="reason-meta">При текущих порогах очистка не нашла колонок на исключение.</div></div>';
    }

    function renderFiles(files) {
        const container = document.getElementById('filesList');
        if (!container) return;
        const items = [
            { label: 'CSV-отчёт по очистке', path: files?.profile_csv || '' },
            { label: 'Excel-отчёт по очистке', path: files?.profile_xlsx || '' },
            { label: 'Excel-файл очищенной таблицы', path: files?.clean_xlsx || '' },
        ].filter((item) => item.path);
        container.innerHTML = items.map((item) => `<div class="file-item"><div class="file-label">${escapeHtml(item.label)}</div><div class="file-path">${escapeHtml(item.path)}</div></div>`).join('');
    }

    function renderCandidates(candidateDetails) {
        const body = document.getElementById('candidatesBody');
        if (!body) return;
        body.innerHTML = Array.isArray(candidateDetails) && candidateDetails.length ? candidateDetails.map((item) => `<tr><td data-label="Колонка">${escapeHtml(item.column)}</td><td data-label="Тип">${escapeHtml(item.dtype)}</td><td data-label="Причины">${escapeHtml((item.reasons || []).join(', '))}</td><td data-label="Пропуски">${escapeHtml(formatPercent(item.null_ratio))}</td><td data-label="Доминирующее значение">${escapeHtml(formatPercent(item.dominant_ratio))}</td></tr>`).join('') : '<tr><td colspan="5">При текущих порогах очистка не нашла колонок на исключение.</td></tr>';
    }

    function renderRemovedColumns(candidateDetails) {
        const section = document.getElementById('columnPairsSection');
        const container = document.getElementById('removedColumns');
        if (!section || !container) return;
        section.hidden = false;
        if (!Array.isArray(candidateDetails) || !candidateDetails.length) {
            container.innerHTML = '<div class="chip chip-success">Ничего не удалено</div>';
            return;
        }
        container.innerHTML = candidateDetails
            .map((item) => `<div class="chip chip-removed" title="${escapeHtml((item.reasons || []).join(', '))}">${escapeHtml(item.column)}</div>`)
            .join('');
    }

    function renderKeptColumns(keptColumns, keptCount) {
        const container = document.getElementById('keptColumnsSecondary');
        if (!container) return;
        if (!Array.isArray(keptColumns) || !keptColumns.length) {
            container.innerHTML = '<div class="chip">Список пока недоступен</div>';
            return;
        }
        container.innerHTML = keptColumns.map((item) => `<div class="chip">${escapeHtml(item)}</div>`).join('');
    }

    function renderSummary(result) {
        const summary = result.summary || {};
        document.getElementById('resultsSection').hidden = false;
        document.getElementById('metricTotal').textContent = summary.total_columns ?? 0;
        document.getElementById('metricRemoved').textContent = summary.candidate_count ?? 0;
        document.getElementById('metricKept').textContent = summary.kept_count ?? 0;
        document.getElementById('metricShape').textContent = `${summary.clean_rows ?? 0} x ${summary.clean_columns ?? 0}`;
        document.getElementById('resultMessage').textContent = result.message || 'Очистка завершена.';
        renderRunSummary({ tableName: result.table_name || selectedTable || 'Не выбрана', removedCount: summary.candidate_count ?? 0, cleanTable: result.clean_table || '-' });
        document.getElementById('outputSummary').innerHTML = `Создана таблица <strong>${escapeHtml(result.clean_table || '')}</strong>. В ней осталось <strong>${escapeHtml(summary.kept_count ?? 0)}</strong> колонок и <strong>${escapeHtml(summary.clean_rows ?? 0)}</strong> строк. ${result.clean_table ? `<br><br>Открыть таблицу можно в разделе <a class="text-link" href="/tables/${encodeURIComponent(result.clean_table)}">Просмотр таблиц</a>.` : ''}`;
        renderThresholds(summary.thresholds || {});
        renderReasonSummary(summary.reason_summary || []);
        renderFiles(result.files || {});
        renderCandidates(summary.candidate_details || []);
        renderRemovedColumns(summary.candidate_details || []);
        renderKeptColumns(summary.kept_columns || [], summary.kept_count || 0);
    }

    async function runPipeline() {
        if (!selectedTable || isRunning) return;
        let thresholds;
        setProfilingLoadingState(
            'Запускаем очистку таблицы',
            'Загружаем данные, проверяем колонки и готовим итоговую сводку.',
            0,
            { showSkeleton: true }
        );
        startProfilingProgressSequence();

        try {
            thresholds = readThresholds();
        } catch (error) {
            clearProfilingStepTimers();
            setStatus('error', escapeHtml(error.message));
            setProfilingProgress(0, {
                lead: 'Не удалось проверить пороги',
                message: error.message,
                isError: true,
            });
            showProfilingError(error.message);
            return;
        }

        isRunning = true;
        document.getElementById('resultsSection').hidden = true;
        document.getElementById('columnPairsSection').hidden = true;
        const runButton = document.getElementById('runButton');
        runButton.disabled = true;
        runButton.classList.add('is-loading');
        runButton.textContent = 'Очистка выполняется...';
        renderRunSummary({ tableName: selectedTable, removedCount: 'Считаем...', cleanTable: `clean_${selectedTable}` });
        setStatus('running', `Выполняем очистку для таблицы <strong>${escapeHtml(selectedTable)}</strong> с пользовательскими порогами.`);

        try {
            const jobId = createJobId();
            const apiResult = await fetchJson('/run_profiling', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ table: selectedTable, thresholds, job_id: jobId }),
            }, 'Не удалось выполнить очистку.');
            const result = apiResult.payload;
            if (result.status !== 'success') throw new Error(result.message || result.status || 'Не удалось выполнить очистку.');
            renderSummary(result);
            setStatus('success', `Готово. Таблица <strong>${escapeHtml(selectedTable)}</strong> обработана по выбранным порогам.`);
            clearProfilingStepTimers();
            setProfilingReadyState(
                'Очистка завершена',
                'Сводка, cleaned-table и файлы результата уже готовы для просмотра.'
            );
        } catch (error) {
            setStatus('error', `Ошибка выполнения: ${escapeHtml(error.message)}`);
            clearProfilingStepTimers();
            setProfilingProgress(2, {
                lead: 'Не удалось завершить очистку',
                message: error.message,
                isError: true,
            });
            showProfilingError(error.message);
        } finally {
            isRunning = false;
            runButton.disabled = false;
            runButton.classList.remove('is-loading');
            runButton.textContent = 'Запустить очистку';
        }
    }

    function initializeProfilingControls() {
        const tableSelect = document.getElementById('tableSelect');
        const runButton = document.getElementById('runButton');

        if (tableSelect) {
            tableSelect.addEventListener('change', (event) => {
                selectTable(event.target.value);
            });
            selectTable(tableSelect.value || '');
        }

        if (runButton) {
            runButton.addEventListener('click', () => {
                if (!runButton.disabled) {
                    runPipeline();
                }
            });
        }
        const retryButton = document.getElementById('profilingRetryButton');
        if (retryButton) {
            retryButton.addEventListener('click', () => {
                if (!isRunning) {
                    runPipeline();
                }
            });
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializeProfilingControls);
    } else {
        initializeProfilingControls();
    }
}());
