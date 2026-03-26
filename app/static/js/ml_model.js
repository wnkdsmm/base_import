(function () {
    var currentMlData = null;
    var isFetching = false;
    var progressTimers = [];
    var progressSteps = [
        {
            label: 'Загрузка данных',
            lead: 'Загружаем данные ML-блока',
            message: 'Получаем выбранный срез и обновляем параметры страницы.'
        },
        {
            label: 'Подготовка истории',
            lead: 'Подготавливаем историю',
            message: 'Собираем дневной ряд, фильтры и доступные признаки.'
        },
        {
            label: 'Расчет модели',
            lead: 'Расчет модели',
            message: 'Считаем backtesting, прогноз и итоговые таблицы.'
        }
    ];

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

    function setValue(id, value) {
        var node = byId(id);
        if (node) {
            node.value = value == null ? '' : value;
        }
    }

    function setSelectOptions(id, options, selectedValue, emptyLabel) {
        var selectNode = byId(id);
        if (!selectNode) {
            return;
        }

        var safeOptions = Array.isArray(options) && options.length ? options : [{ value: 'all', label: emptyLabel }];
        selectNode.innerHTML = safeOptions.map(function (option) {
            var selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        }).join('');
    }

    function normalizePercent(value, fallback) {
        var normalizedFallback = fallback || '0%';
        var rawValue = String(value == null ? '' : value).trim();
        var match = rawValue.match(/^(-?\d+(?:\.\d+)?)%?$/);
        if (!match) {
            return normalizedFallback;
        }

        var numericValue = Math.max(0, Math.min(100, Number(match[1])));
        return numericValue + '%';
    }

    function normalizeCssColor(value, fallback) {
        var normalizedFallback = fallback || 'currentColor';
        var candidate = String(value == null ? '' : value).trim();
        if (!candidate) {
            return normalizedFallback;
        }

        var probe = document.createElement('span');
        probe.style.color = '';
        probe.style.color = candidate;
        return probe.style.color ? candidate : normalizedFallback;
    }

    function applyChartDecorators(root) {
        var scope = root && typeof root.querySelectorAll === 'function' ? root : document;
        Array.prototype.forEach.call(scope.querySelectorAll('[data-legend-color]'), function (node) {
            node.style.setProperty('--legend-color', normalizeCssColor(node.getAttribute('data-legend-color'), 'currentColor'));
        });
        Array.prototype.forEach.call(scope.querySelectorAll('[data-bar-width]'), function (node) {
            node.style.setProperty('--ml-bar-width', normalizePercent(node.getAttribute('data-bar-width'), '0%'));
        });
    }

    function formatDate(value) {
        if (!value || value.length < 10) {
            return value || '';
        }
        return value.slice(8, 10) + '.' + value.slice(5, 7);
    }

    function renderFallback(chartNode, fallbackNode, message) {
        if (!chartNode || !fallbackNode) {
            return;
        }
        chartNode.innerHTML = '';
        fallbackNode.textContent = message || 'Нет данных для графика.';
        fallbackNode.classList.remove('is-hidden');
    }

    function renderLineChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var series = chart && chart.series;
        if (!series || !Array.isArray(series.history) || !series.history.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        fallbackNode.classList.add('is-hidden');
        var history = series.history || [];
        var backtestActual = series.backtest_actual || [];
        var backtestPredicted = series.backtest_predicted || [];
        var forecast = series.forecast || [];
        var band = series.forecast_band || [];
        var orderedDates = [];
        var seen = {};

        [history, backtestActual, backtestPredicted, forecast].forEach(function (points) {
            points.forEach(function (point) {
                if (!seen[point.x]) {
                    seen[point.x] = true;
                    orderedDates.push(point.x);
                }
            });
        });

        if (!orderedDates.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        var xIndex = {};
        orderedDates.forEach(function (date, index) {
            xIndex[date] = index;
        });

        var values = [];
        history.forEach(function (point) { values.push(point.y); });
        backtestActual.forEach(function (point) { values.push(point.y); });
        backtestPredicted.forEach(function (point) { values.push(point.y); });
        forecast.forEach(function (point) { values.push(point.y); });
        band.forEach(function (point) {
            values.push(point.low);
            values.push(point.high);
        });

        var isPercent = chart && chart.value_format === 'percent';
        var yMin = 0;
        var yMax = Math.max.apply(null, values.concat([1]));
        if (isPercent) {
            yMax = Math.min(100, Math.max(25, Math.ceil((yMax + 5) / 5) * 5));
        } else {
            yMax = Math.max(1, Math.ceil((yMax + 0.5) * 2) / 2);
        }
        if (yMax <= yMin) {
            yMax = yMin + 1;
        }

        var width = 920;
        var height = 420;
        var padding = { top: 20, right: 24, bottom: 54, left: 54 };
        var innerWidth = width - padding.left - padding.right;
        var innerHeight = height - padding.top - padding.bottom;
        var denominator = Math.max(orderedDates.length - 1, 1);

        function x(date) {
            return padding.left + (xIndex[date] / denominator) * innerWidth;
        }

        function y(value) {
            return padding.top + innerHeight - ((value - yMin) / (yMax - yMin)) * innerHeight;
        }

        function linePath(points) {
            var path = '';
            points.forEach(function (point, index) {
                var px = x(point.x);
                var py = y(point.y);
                path += (index ? ' L ' : 'M ') + px.toFixed(2) + ' ' + py.toFixed(2);
            });
            return path;
        }

        var axisLines = '';
        var gridLines = '';
        for (var step = 0; step <= 4; step += 1) {
            var value = yMin + ((yMax - yMin) * step / 4);
            var py = y(value);
            gridLines += '<line x1="' + padding.left + '" y1="' + py.toFixed(2) + '" x2="' + (width - padding.right) + '" y2="' + py.toFixed(2) + '" class="ml-grid-line"></line>';
            var axisValue = Math.round(value * 10) / 10;
            var axisText = isPercent
                ? String(axisValue).replace('.', ',') + '%'
                : String(axisValue).replace('.', ',');
            axisLines += '<text x="' + (padding.left - 10) + '" y="' + (py + 4).toFixed(2) + '" text-anchor="end" class="ml-axis-label">' + escapeHtml(axisText) + '</text>';
        }

        var tickIndexes = [0, Math.floor((orderedDates.length - 1) / 2), orderedDates.length - 1]
            .filter(function (value, index, arr) { return arr.indexOf(value) === index && value >= 0; });
        tickIndexes.forEach(function (index) {
            var tickDate = orderedDates[index];
            axisLines += '<text x="' + x(tickDate).toFixed(2) + '" y="' + (height - 16) + '" text-anchor="middle" class="ml-axis-label">' + escapeHtml(formatDate(tickDate)) + '</text>';
        });

        var bandPath = '';
        if (band.length) {
            var upper = band.map(function (point) { return x(point.x).toFixed(2) + ' ' + y(point.high).toFixed(2); }).join(' L ');
            var lower = band.slice().reverse().map(function (point) { return x(point.x).toFixed(2) + ' ' + y(point.low).toFixed(2); }).join(' L ');
            bandPath = '<path d="M ' + upper + ' L ' + lower + ' Z" class="ml-band-path"></path>';
        }

        var svg = ''
            + '<svg viewBox="0 0 ' + width + ' ' + height + '" class="ml-svg-chart" preserveAspectRatio="none">'
            + gridLines
            + '<line x1="' + padding.left + '" y1="' + (height - padding.bottom) + '" x2="' + (width - padding.right) + '" y2="' + (height - padding.bottom) + '" class="ml-axis-line"></line>'
            + '<line x1="' + padding.left + '" y1="' + padding.top + '" x2="' + padding.left + '" y2="' + (height - padding.bottom) + '" class="ml-axis-line"></line>'
            + bandPath;

        if (history.length) {
            svg += '<path d="' + linePath(history) + '" class="ml-line-history"></path>';
        }
        if (backtestActual.length) {
            svg += '<path d="' + linePath(backtestActual) + '" class="ml-line-backtest-actual"></path>';
        }
        if (backtestPredicted.length) {
            svg += '<path d="' + linePath(backtestPredicted) + '" class="ml-line-backtest"></path>';
        }
        if (forecast.length) {
            svg += '<path d="' + linePath(forecast) + '" class="ml-line-forecast"></path>';
        }
        forecast.forEach(function (point) {
            svg += '<circle cx="' + x(point.x).toFixed(2) + '" cy="' + y(point.y).toFixed(2) + '" r="3.5" class="ml-forecast-point"></circle>';
        });
        svg += axisLines + '</svg>';

        var legend = '';
        if (Array.isArray(chart.legend) && chart.legend.length) {
            legend = '<div class="ml-chart-legend">' + chart.legend.map(function (item) {
                return '<span class="ml-chart-legend-item"><i data-legend-color="' + escapeHtml(item.color) + '"></i>' + escapeHtml(item.label) + '</span>';
            }).join('') + '</div>';
        }

        chartNode.innerHTML = legend + '<div class="ml-chart-shell">' + svg + '</div>';
        applyChartDecorators(chartNode);
    }

    function renderBarsChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var items = chart && chart.items;
        if (!Array.isArray(items) || !items.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        fallbackNode.classList.add('is-hidden');
        var maxValue = Math.max.apply(null, items.map(function (item) { return item.value; }).concat([1]));
        var html = '<div class="ml-bars">';
        items.forEach(function (item) {
            var percent = Math.max(8, Math.round((item.value / maxValue) * 100));
            html += ''
                + '<div class="ml-bar-row">'
                + '<div class="ml-bar-meta"><span>' + escapeHtml(item.label) + '</span><strong>' + escapeHtml(item.value_display) + '%</strong></div>'
                + '<div class="ml-bar-track"><div class="ml-bar-fill" data-bar-width="' + percent + '%"></div></div>'
                + '</div>';
        });
        html += '</div>';
        chartNode.innerHTML = html;
        applyChartDecorators(chartNode);
    }

    function renderSidebarStatus(data) {
        var container = byId('mlSidebarStatus');
        if (!container) {
            return;
        }

        var summary = data && data.summary ? data.summary : {};
        var badgeClass = 'status-badge';
        if (data && data.has_data && !data.error_message) {
            badgeClass += ' status-badge-live';
        }

        var badgeLabel = 'Нужно уточнить фильтры';
        if (data && data.error_message) {
            badgeLabel = 'Требуется повторный расчет';
        } else if (isFetching || (data && data.bootstrap_mode === 'deferred')) {
            badgeLabel = 'Подготовка ML-блока';
        } else if (data && data.has_data) {
            badgeLabel = 'ML-блок собран';
        }

        container.innerHTML = ''
            + '<span class="' + badgeClass + '">' + escapeHtml(badgeLabel) + '</span>'
            + '<div class="status-line"><span>Модель по числу пожаров</span><strong>' + escapeHtml(summary.count_model_label || 'Регрессия Пуассона') + '</strong></div>'
            + '<div class="status-line"><span>Событие пожара</span><strong>' + escapeHtml(summary.event_model_label || 'Не обучен') + '</strong></div>'
            + '<div class="status-line"><span>Проверка на истории</span><strong>' + escapeHtml(summary.backtest_method_label || 'Проверка на истории не выполнена') + '</strong></div>'
            + '<div class="status-line"><span>Период</span><strong>' + escapeHtml(summary.history_period_label || 'Нет данных') + '</strong></div>';
    }

    function renderHero(data) {
        var summary = data.summary || {};
        setText('mlModelDescription', data.model_description || 'После загрузки здесь появится описание ML-блока и качества модели.');

        var heroTags = byId('mlHeroTags');
        if (heroTags) {
            heroTags.innerHTML = ''
                + '<span class="hero-tag">Таблица: <strong>' + escapeHtml(summary.selected_table_label || 'Нет таблицы') + '</strong></span>'
                + '<span class="hero-tag">История: <strong>' + escapeHtml(summary.history_window_label || 'Все годы') + '</strong></span>'
                + '<span class="hero-tag">Топ-признак: <strong>' + escapeHtml(summary.top_feature_label || '-') + '</strong></span>'
                + '<span class="hero-tag">Температура: <strong>' + escapeHtml(summary.temperature_scenario_display || 'Историческая температура') + '</strong></span>'
                + '<span class="hero-tag">'
                + (summary.event_probability_enabled
                    ? 'Средняя вероятность P(>=1 пожара): <strong>' + escapeHtml(summary.average_event_probability_display || '—') + '</strong>'
                    : 'Событие пожара: <strong>не показано</strong>')
                + '</span>';
        }

        var heroStats = byId('mlHeroStats');
        if (heroStats) {
            heroStats.innerHTML = ''
                + '<article class="hero-stat-card">'
                + '<span class="hero-stat-label">Среднее ожидаемое число пожаров</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.average_expected_count_display || '0') + '</strong>'
                + '<span class="hero-stat-foot">Средняя дневная интенсивность на выбранном горизонте прогноза.</span>'
                + '</article>'
                + '<article class="hero-stat-card hero-stat-card-soft">'
                + '<span class="hero-stat-label">Пиковый день</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.peak_expected_count_display || '0') + '</strong>'
                + '<span class="hero-stat-foot">Максимальное ожидаемое число пожаров: ' + escapeHtml(summary.peak_expected_count_day_display || '-') + '.</span>'
                + '</article>';
        }
    }

    function renderSummaryCards(summary) {
        var container = byId('mlStatsGrid');
        if (!container) {
            return;
        }

        container.innerHTML = ''
            + '<article class="stat-card stat-card-accent">'
            + '<span class="stat-label">Пожаров в обучении</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.fires_count_display || '0') + '</strong>'
            + '<span class="stat-foot">После выбранных фильтров.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Дней истории</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.history_days_display || '0') + '</strong>'
            + '<span class="stat-foot">Непрерывный дневной ряд с нулями между пожарами.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Сумма прогноза</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.predicted_total_display || '0') + '</strong>'
            + '<span class="stat-foot">Ожидаемое число пожаров на всем горизонте.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Дней с повышенным индексом</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.elevated_risk_days_display || '0') + '</strong>'
            + '<span class="stat-foot">Количество дней, где риск-индекс не ниже 75/100.</span>'
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

    function renderModelChoice(modelChoice) {
        var safeChoice = modelChoice || {};
        setText('mlModelChoiceTitle', safeChoice.title || 'Почему выбрана лучшая модель');
        setText('mlModelChoiceLead', safeChoice.lead || 'После валидации здесь появится краткое объяснение выбора модели.');
        setText('mlModelChoiceBody', safeChoice.body || 'Недостаточно данных, чтобы обосновать выбор count-модели.');

        var factsContainer = byId('mlModelChoiceFacts');
        if (!factsContainer) {
            return;
        }

        var facts = Array.isArray(safeChoice.facts) ? safeChoice.facts : [];
        if (!facts.length) {
            factsContainer.innerHTML = '<div class="mini-empty">После проверки здесь появятся факты по выбранной модели.</div>';
            return;
        }

        factsContainer.innerHTML = facts.map(function (item) {
            return ''
                + '<article class="stat-card">'
                + '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>'
                + '<strong class="stat-value">' + escapeHtml(item.value || '-') + '</strong>'
                + '</article>';
        }).join('');
    }

    function renderCountTable(table) {
        var container = byId('mlCountTableShell');
        var safeTable = table || {};
        var rows = Array.isArray(safeTable.rows) ? safeTable.rows : [];
        if (!container) {
            return;
        }

        if (!rows.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(safeTable.empty_message || 'Сравнение baseline, сценарного прогноза и count-моделей появится после проверки на истории.') + '</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="forecast-table">'
            + '<thead><tr><th>Метод</th><th>Роль</th><th>MAE</th><th>RMSE</th><th>SMAPE</th><th>Девиация Пуассона</th><th>MAE к базовой модели</th><th>Статус</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Метод">' + escapeHtml(row.method_label || '-') + '</td>'
                    + '<td data-label="Роль">' + escapeHtml(row.role_label || '-') + '</td>'
                    + '<td data-label="MAE">' + escapeHtml(row.mae_display || '-') + '</td>'
                    + '<td data-label="RMSE">' + escapeHtml(row.rmse_display || '-') + '</td>'
                    + '<td data-label="SMAPE">' + escapeHtml(row.smape_display || '-') + '</td>'
                    + '<td data-label="Девиация Пуассона">' + escapeHtml(row.poisson_display || '-') + '</td>'
                    + '<td data-label="MAE к базовой модели">' + escapeHtml(row.mae_delta_display || '-') + '</td>'
                    + '<td data-label="Статус">' + escapeHtml(row.selection_label || '-') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderEventTable(table) {
        var container = byId('mlEventTableShell');
        var safeTable = table || {};
        var rows = Array.isArray(safeTable.rows) ? safeTable.rows : [];
        if (!container) {
            return;
        }

        if (!rows.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(safeTable.empty_message || 'Недостаточно окон для сравнения вероятности события пожара.') + '</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="forecast-table">'
            + '<thead><tr><th>Метод</th><th>Роль</th><th>Показатель Брайера</th><th>ROC-AUC</th><th>F1</th><th>Статус</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Метод">' + escapeHtml(row.method_label || '-') + '</td>'
                    + '<td data-label="Роль">' + escapeHtml(row.role_label || '-') + '</td>'
                    + '<td data-label="Показатель Брайера">' + escapeHtml(row.brier_display || '-') + '</td>'
                    + '<td data-label="ROC-AUC">' + escapeHtml(row.roc_auc_display || '-') + '</td>'
                    + '<td data-label="F1">' + escapeHtml(row.f1_display || '-') + '</td>'
                    + '<td data-label="Статус">' + escapeHtml(row.selection_label || '-') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderForecastTable(rows) {
        var container = byId('mlForecastTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">После обучения здесь появится прогноз по будущим датам.</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="forecast-table">'
            + '<thead><tr><th>Дата</th><th>Ожидаемое число пожаров</th><th>Диапазон</th><th>Индекс риска</th><th>P(&gt;=1 пожара)</th><th>Температура</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Дата">' + escapeHtml(row.date_display || '-') + '</td>'
                    + '<td data-label="Ожидаемое число пожаров">' + escapeHtml(row.forecast_value_display || '0') + '</td>'
                    + '<td data-label="Диапазон">' + escapeHtml(row.range_display || '—') + '</td>'
                    + '<td data-label="Индекс риска"><span class="ml-risk-pill ml-risk-' + escapeHtml(row.risk_level_tone || 'minimal') + '">' + escapeHtml(row.risk_index_display || '0 / 100') + '</span></td>'
                    + '<td data-label="P(>=1 пожара)">' + escapeHtml(row.event_probability_display || '—') + '</td>'
                    + '<td data-label="Температура">' + escapeHtml(row.temperature_display || '—') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderFeatureCards(items) {
        var container = byId('mlFeatureCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">После выбора таблицы здесь появятся признаки модели.</div>';
            return;
        }

        container.innerHTML = items.map(function (feature) {
            return ''
                + '<article class="forecast-feature-card status-' + escapeHtml(feature.status || 'missing') + '">'
                + '<div class="forecast-feature-head">'
                + '<strong>' + escapeHtml(feature.label || '-') + '</strong>'
                + '<span class="forecast-badge">' + escapeHtml(feature.status_label || '-') + '</span>'
                + '</div>'
                + '<div class="forecast-feature-source">Источник: ' + escapeHtml(feature.source || '-') + '</div>'
                + '<p>' + escapeHtml(feature.description || '') + '</p>'
                + '</article>';
        }).join('');
    }

    function renderStatsSkeletons() {
        var container = byId('mlStatsGrid');
        if (!container) {
            return;
        }

        container.innerHTML = [0, 1, 2, 3].map(function (index) {
            return ''
                + '<article class="stat-card' + (index === 0 ? ' stat-card-accent' : '') + ' ml-skeleton-card">'
                + '<span class="ml-skeleton-line short"></span>'
                + '<span class="ml-skeleton-line value"></span>'
                + '<span class="ml-skeleton-line long"></span>'
                + '</article>';
        }).join('');
    }

    function renderCardSkeletons(containerId, count) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        var items = [];
        for (var index = 0; index < count; index += 1) {
            items.push(''
                + '<article class="stat-card ml-skeleton-card">'
                + '<span class="ml-skeleton-line short"></span>'
                + '<span class="ml-skeleton-line value"></span>'
                + '<span class="ml-skeleton-line long"></span>'
                + '</article>');
        }
        container.innerHTML = items.join('');
    }

    function renderModelChoiceSkeleton() {
        setText('mlModelChoiceTitle', 'Почему выбрана лучшая модель');
        setText('mlModelChoiceLead', 'Готовим объяснение выбора модели по rolling-origin backtesting.');
        var bodyNode = byId('mlModelChoiceBody');
        if (bodyNode) {
            bodyNode.innerHTML = ''
                + '<span class="ml-skeleton-line long"></span>'
                + '<span class="ml-skeleton-line long"></span>'
                + '<span class="ml-skeleton-line medium"></span>';
        }
        renderCardSkeletons('mlModelChoiceFacts', 3);
    }

    function renderTableSkeleton(containerId, columns, rows) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        var rowHtml = [];
        for (var rowIndex = 0; rowIndex < rows; rowIndex += 1) {
            var cells = [];
            for (var columnIndex = 0; columnIndex < columns; columnIndex += 1) {
                cells.push('<span class="ml-skeleton-table-cell"></span>');
            }
            rowHtml.push('<div class="ml-skeleton-table-row" style="--ml-skeleton-cols:' + columns + '">' + cells.join('') + '</div>');
        }
        container.innerHTML = '<div class="ml-skeleton-table">' + rowHtml.join('') + '</div>';
    }

    function renderChartSkeleton(chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (chartNode) {
            chartNode.innerHTML = '<div class="ml-chart-placeholder"></div>';
        }
        if (fallbackNode) {
            fallbackNode.classList.add('is-hidden');
        }
    }

    function renderFeatureSkeleton() {
        var container = byId('mlFeatureCards');
        if (!container) {
            return;
        }

        container.innerHTML = ''
            + '<div class="ml-skeleton-feature-list">'
            + [0, 1, 2, 3].map(function () {
                return ''
                    + '<article class="forecast-feature-card ml-skeleton-feature">'
                    + '<span class="ml-skeleton-line short"></span>'
                    + '<span class="ml-skeleton-line medium"></span>'
                    + '<span class="ml-skeleton-line long"></span>'
                    + '</article>';
            }).join('')
            + '</div>';
    }

    function renderListSkeleton(containerId, count) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        var items = [];
        for (var index = 0; index < count; index += 1) {
            items.push('<li><span class="ml-skeleton-line long"></span></li>');
        }
        container.innerHTML = items.join('');
    }

    function showInitialSkeletons() {
        renderStatsSkeletons();
        renderCardSkeletons('mlQualityMetricCards', 4);
        renderCardSkeletons('mlQualityMethodology', 5);
        renderModelChoiceSkeleton();
        renderTableSkeleton('mlCountTableShell', 8, 4);
        renderTableSkeleton('mlEventTableShell', 6, 3);
        renderListSkeleton('mlDissertationPoints', 5);
        renderChartSkeleton('mlForecastChart', 'mlForecastChartFallback');
        renderTableSkeleton('mlForecastTableShell', 6, 4);
        renderChartSkeleton('mlImportanceChart', 'mlImportanceChartFallback');
        renderFeatureSkeleton();
        renderListSkeleton('mlNotesList', 5);
    }

    function applyMlModelData(data) {
        if (!data) {
            return;
        }

        currentMlData = data;

        var filters = data.filters || {};
        var summary = data.summary || {};
        var quality = data.quality_assessment || {};
        var charts = data.charts || {};

        renderSidebarStatus(data);
        renderHero(data);
        renderSummaryCards(summary);

        setSelectOptions('mlTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('mlHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Все годы');
        setSelectOptions('mlCauseFilter', filters.available_causes, filters.cause, 'Все причины');
        setSelectOptions('mlObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Все категории');
        setSelectOptions('mlForecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 дней');
        setValue('mlTemperatureInput', filters.temperature || '');

        setText('mlQualityTitle', quality.title || 'Оценка качества ML-блока');
        setText('mlQualitySubtitle', quality.subtitle || 'На одной и той же истории сравниваются baseline, сценарная эвристика и интерпретируемые count-модели; основной критерий — rolling-origin backtesting.');
        renderMetricCards('mlQualityMetricCards', quality.metric_cards || [], 'После расчета здесь появятся метрики качества ML-блока.');
        renderMetricCards('mlQualityMethodology', quality.methodology_items || [], 'Параметры валидации появятся после проверки на истории.');
        renderModelChoice(quality.model_choice || {});
        setText('mlCountTableTitle', quality.count_table && quality.count_table.title ? quality.count_table.title : 'Сравнение по числу пожаров');
        renderCountTable(quality.count_table || {});
        setText('mlEventTableTitle', quality.event_table && quality.event_table.title ? quality.event_table.title : 'Сравнение по вероятности события пожара');
        renderEventTable(quality.event_table || {});
        renderNoticeList('mlDissertationPoints', quality.dissertation_points || [], 'После проверки на истории здесь появятся готовые формулировки для раздела «оценка качества».');

        setText('mlForecastTitle', charts.forecast && charts.forecast.title ? charts.forecast.title : 'ML-прогноз ожидаемого числа пожаров');
        renderLineChart(charts.forecast, 'mlForecastChart', 'mlForecastChartFallback');
        renderForecastTable(data.forecast_rows || []);

        setText('mlImportanceTitle', charts.importance && charts.importance.title ? charts.importance.title : 'Важность признаков ML-блока');
        renderBarsChart(charts.importance, 'mlImportanceChart', 'mlImportanceChartFallback');
        renderFeatureCards(data.features || []);
        renderNoticeList('mlNotesList', data.notes || [], 'Замечаний пока нет.');
    }

    function clearProgressTimers() {
        while (progressTimers.length) {
            clearTimeout(progressTimers.pop());
        }
    }

    function updateProgressStep(activeIndex) {
        var stepsContainer = byId('mlProgressSteps');
        var leadNode = byId('mlLoadingLead');
        var messageNode = byId('mlLoadingMessage');
        var activeStep = progressSteps[Math.max(0, Math.min(progressSteps.length - 1, activeIndex))];

        if (leadNode) {
            leadNode.textContent = activeStep.lead;
        }
        if (messageNode) {
            messageNode.textContent = activeStep.message;
        }
        if (!stepsContainer) {
            return;
        }

        Array.prototype.forEach.call(stepsContainer.querySelectorAll('.ml-progress-step'), function (node, index) {
            node.classList.toggle('is-active', index === activeIndex);
            node.classList.toggle('is-done', index < activeIndex);
        });
    }

    function startProgressSequence() {
        clearProgressTimers();
        updateProgressStep(0);
        progressTimers.push(setTimeout(function () { updateProgressStep(1); }, 350));
        progressTimers.push(setTimeout(function () { updateProgressStep(2); }, 1100));
    }

    function setRefreshButtonState(isBusy) {
        var button = byId('mlRefreshButton');
        if (!button) {
            return;
        }
        button.disabled = !!isBusy;
        button.classList.toggle('is-loading', !!isBusy);
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
            errorState.textContent = '';
            errorState.classList.add('is-hidden');
        }
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
        var errorState = byId('mlErrorState');
        if (asyncState) {
            asyncState.classList.remove('is-hidden');
        }
        if (errorState) {
            errorState.textContent = message || 'Не удалось загрузить ML-данные. Попробуйте еще раз.';
            errorState.classList.remove('is-hidden');
        }
    }

    function hideError() {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        if (errorState) {
            errorState.textContent = '';
            errorState.classList.add('is-hidden');
        }
        if (asyncState && loadingState && loadingState.classList.contains('is-hidden')) {
            asyncState.classList.add('is-hidden');
        }
    }

    function buildQueryFromForm() {
        var form = byId('mlModelForm');
        if (!form) {
            return '';
        }
        return new URLSearchParams(new FormData(form)).toString();
    }

    async function fetchMlModelData(options) {
        var settings = options || {};
        var query = settings.useLocationSearch && window.location.search
            ? window.location.search.replace(/^\?/, '')
            : buildQueryFromForm();
        var endpoint = '/api/ml-model-data' + (query ? '?' + query : '');
        var response;
        var payload = null;

        isFetching = true;
        setRefreshButtonState(true);
        showLoadingState();
        startProgressSequence();
        renderSidebarStatus(currentMlData || window.__FIRE_ML_INITIAL__ || {});

        if (settings.initialLoad) {
            showInitialSkeletons();
        }

        try {
            response = await fetch(endpoint, { headers: { Accept: 'application/json' } });
            payload = await response.json();

            if (!response.ok) {
                if (payload) {
                    applyMlModelData(payload);
                }
                throw new Error(payload && payload.error_message ? payload.error_message : 'Не удалось рассчитать ML-модель.');
            }

            applyMlModelData(payload);
            hideError();
            window.history.replaceState({}, '', query ? (window.location.pathname + '?' + query) : window.location.pathname);
        } catch (error) {
            console.error(error);
            showError(error && error.message ? error.message : 'Не удалось загрузить ML-данные. Попробуйте еще раз.');
        } finally {
            clearProgressTimers();
            isFetching = false;
            setRefreshButtonState(false);
            hideLoadingState();
            renderSidebarStatus(currentMlData || window.__FIRE_ML_INITIAL__ || {});
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        var form = byId('mlModelForm');
        var initialData = window.__FIRE_ML_INITIAL__ || null;

        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchMlModelData();
            });
        }

        if (initialData && initialData.bootstrap_mode !== 'deferred') {
            applyMlModelData(initialData);
        } else {
            applyMlModelData(initialData || {});
            fetchMlModelData({ initialLoad: true, useLocationSearch: true });
        }
    });
})();
