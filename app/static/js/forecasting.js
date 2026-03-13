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

    function renderFeatures(features) {
        var container = byId('forecastFeatures');
        if (!container) {
            return;
        }

        if (!Array.isArray(features) || !features.length) {
            container.innerHTML = '<div class="mini-empty">Признаки появятся после выбора таблицы.</div>';
            return;
        }

        container.innerHTML = features.map(function (feature) {
            return '<article class="forecast-feature-card status-' + escapeHtml(feature.status || 'missing') + '">' +
                '<div class="forecast-feature-head">' +
                    '<strong>' + escapeHtml(feature.label) + '</strong>' +
                    '<span class="forecast-badge">' + escapeHtml(feature.status_label) + '</span>' +
                '</div>' +
                '<div class="forecast-feature-source">Источник: ' + escapeHtml(feature.source) + '</div>' +
                '<p>' + escapeHtml(feature.description) + '</p>' +
            '</article>';
        }).join('');
    }

    function renderInsights(items) {
        var container = byId('forecastInsights');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Сигналы появятся после расчета прогноза.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="insight-card tone-' + escapeHtml(item.tone || 'fire') + '">' +
                '<span class="insight-label">' + escapeHtml(item.label) + '</span>' +
                '<strong class="insight-value">' + escapeHtml(item.value) + '</strong>' +
                '<span class="insight-meta">' + escapeHtml(item.meta) + '</span>' +
            '</article>';
        }).join('');
    }

    function renderNotes(notes) {
        var container = byId('forecastNotesList');
        if (!container) {
            return;
        }

        if (!Array.isArray(notes) || !notes.length) {
            container.innerHTML = '<li>Замечаний пока нет.</li>';
            return;
        }

        container.innerHTML = notes.map(function (note) {
            return '<li>' + escapeHtml(note) + '</li>';
        }).join('');
    }

    function renderForecastTable(rows) {
        var container = byId('forecastTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">После расчета здесь появятся будущие даты и ожидаемое количество пожаров.</div>';
            return;
        }

        container.innerHTML = '<table class="forecast-table">' +
            '<thead><tr><th>Дата</th><th>Прогноз пожаров</th><th>Диапазон</th><th>Температура</th></tr></thead>' +
            '<tbody>' + rows.map(function (row) {
                return '<tr>' +
                    '<td>' + escapeHtml(row.date_display) + '</td>' +
                    '<td>' + escapeHtml(row.forecast_value_display) + '</td>' +
                    '<td>' + escapeHtml(String(row.lower_bound_display || row.lower_bound) + ' - ' + String(row.upper_bound_display || row.upper_bound)) + '</td>' +
                    '<td>' + escapeHtml(row.temperature_display) + '</td>' +
                '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var figure = chart && chart.plotly;
        if (!figure || !window.Plotly || !Array.isArray(figure.data) || !figure.data.length) {
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            chartNode.innerHTML = '';
            return;
        }

        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(
            chartNode,
            figure.data || [],
            figure.layout || {},
            figure.config || { responsive: true }
        );
    }

    function applyForecastData(data) {
        if (!data) {
            return;
        }

        var filters = data.filters || {};
        var summary = data.summary || {};
        var charts = data.charts || {};

        setSelectOptions('forecastTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('forecastDistrictFilter', filters.available_districts, filters.district, 'Все районы');
        setSelectOptions('forecastCauseFilter', filters.available_causes, filters.cause, 'Все причины');
        setSelectOptions('forecastObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Все категории');
        setSelectOptions('forecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 дней');
        setValue('forecastTemperatureInput', filters.temperature || '');

        setText('forecastModelDescription', data.model_description || '');
        setText('forecastTableLabel', summary.selected_table_label || 'Нет таблицы');
        setText('forecastSliceLabel', summary.slice_label || 'Все пожары');
        setText('forecastTemperatureMode', summary.temperature_scenario_display || 'Историческая сезонность');
        setText('forecastHistoryRange', summary.history_period_label || 'Нет данных');
        setText('forecastLastObserved', summary.last_observed_date || '-');
        setText('forecastAverageValue', summary.predicted_average_display || '0');
        setText('forecastTotalValue', summary.predicted_total_display || '0');
        setText('forecastFiresCount', summary.fires_count_display || '0');
        setText('forecastHistoryDays', summary.history_days_display || '0');
        setText('forecastWeeklyTotal', summary.weekly_forecast_display || '0');
        setText('forecastMonthlyTotal', summary.monthly_forecast_display || '0');
        setText('forecastSidebarTable', summary.selected_table_label || 'Нет таблицы');
        setText('forecastSidebarHistory', summary.history_period_label || 'Нет данных');
        setText('forecastSidebarHorizon', (summary.forecast_days_display || '0') + ' дн.');

        setText('forecastDailyChartTitle', charts.daily ? charts.daily.title : 'Прогноз количества пожаров по датам');
        setText('forecastWeeklyChartTitle', charts.weekly ? charts.weekly.title : 'Недельный прогноз');
        setText('forecastMonthlyChartTitle', charts.monthly ? charts.monthly.title : 'Месячный сценарий');
        setText('forecastWeekdayChartTitle', charts.weekday ? charts.weekday.title : 'Ритм по дням недели');

        var summaryNode = byId('forecastSummaryLine');
        if (summaryNode) {
            summaryNode.textContent = (summary.slice_label || 'Все пожары') + ' | История: ' + (summary.history_period_label || 'Нет данных') + ' | Горизонт: ' + (summary.forecast_days_display || '0') + ' дней';
        }

        renderInsights(data.insights || []);
        renderFeatures(data.features || []);
        renderNotes(data.notes || []);
        renderForecastTable(data.forecast_rows || []);
        renderChart(charts.daily, 'forecastDailyChart', 'forecastDailyChartFallback');
        renderChart(charts.weekly, 'forecastWeeklyChart', 'forecastWeeklyChartFallback');
        renderChart(charts.monthly, 'forecastMonthlyChart', 'forecastMonthlyChartFallback');
        renderChart(charts.weekday, 'forecastWeekdayChart', 'forecastWeekdayChartFallback');
    }

    async function fetchForecastData() {
        var form = byId('forecastForm');
        var button = byId('forecastRefreshButton');
        if (!form) {
            return;
        }

        var params = new URLSearchParams(new FormData(form));
        var query = params.toString();

        if (button) {
            button.disabled = true;
        }

        try {
            var response = await fetch('/api/forecasting-data?' + query, {
                headers: { 'Accept': 'application/json' }
            });

            if (!response.ok) {
                throw new Error('Не удалось пересчитать прогноз');
            }

            var data = await response.json();
            applyForecastData(data);
            window.history.replaceState({}, '', query ? '/forecasting?' + query : '/forecasting');
        } catch (error) {
            console.error(error);
            form.submit();
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        var form = byId('forecastForm');
        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchForecastData();
            });
        }

        var initialData = window.__FIRE_FORECAST_INITIAL__;
        if (initialData) {
            applyForecastData(initialData);
        }
    });
})();
