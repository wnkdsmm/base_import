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
            '<thead><tr><th>Дата</th><th>День недели</th><th>Вероятность пожара</th><th>Комментарий</th></tr></thead>' +
            '<tbody>' + rows.map(function (row) {
                return '<tr>' +
                    '<td>' + escapeHtml(row.date_display) + '</td>' +
                    '<td>' + escapeHtml(row.weekday_label) + '</td>' +
                    '<td>' + escapeHtml(row.fire_probability_display || '0%') + '</td>' +
                    '<td><span class="forecast-scenario-pill tone-' + escapeHtml(row.scenario_tone || 'sky') + '">' + escapeHtml(row.scenario_label || 'Около обычного') + '</span><div class="forecast-cell-note">' + escapeHtml(row.scenario_hint || '') + '</div></td>' +
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
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
    }

    function applyForecastData(data) {
        if (!data) {
            return;
        }

        var filters = data.filters || {};
        var summary = data.summary || {};
        var charts = data.charts || {};

        setSelectOptions('forecastTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('forecastHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Все годы');
        setSelectOptions('forecastCauseFilter', filters.available_causes, filters.cause, 'Все причины');
        setSelectOptions('forecastObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Все категории');
        setSelectOptions('forecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 дней');
        setValue('forecastTemperatureInput', filters.temperature || '');

        setText('forecastModelDescription', data.model_description || '');
        setText('forecastTableLabel', summary.selected_table_label || 'Нет таблицы');
        setText('forecastHistoryMode', summary.history_window_label || 'Все годы');
        setText('forecastSliceLabel', summary.slice_label || 'Все пожары');
        setText('forecastTemperatureMode', summary.temperature_scenario_display || 'Историческая сезонность');
        setText('forecastAverageValue', summary.average_probability_display || '0%');
        setText('forecastDaysTotal', summary.forecast_days_display || '0');
        setText('forecastChangeValue', summary.forecast_vs_recent_display || '0%');
        setText('forecastChangeLabel', summary.forecast_vs_recent_label || 'Нет сравнения');
        setText('forecastFiresCount', summary.fires_count_display || '0');
        setText('forecastHistoryDays', summary.history_days_display || '0');
        setText('forecastActiveDays', summary.active_days_display || '0');
        setText('forecastActiveDaysShare', summary.active_days_share_display || '0%');
        setText('forecastHistoricalAverage', summary.historical_average_display || '0');
        setText('forecastRecentAverage', summary.recent_average_display || '0');
        setText('forecastPeakDay', summary.peak_forecast_day_display || '-');
        setText('forecastPeakValue', summary.peak_forecast_probability_display || '0%');
        setText('forecastPeakRiskDay', summary.peak_forecast_day_display || '-');
        setText('forecastPeakRiskValue', summary.peak_forecast_probability_display || '0%');
        setText('forecastSidebarTable', summary.selected_table_label || 'Нет таблицы');
        setText('forecastSidebarHistory', summary.history_period_label || 'Нет данных');
        setText('forecastSidebarHorizon', (summary.forecast_days_display || '0') + ' дн.');

        setText('forecastDailyChartTitle', charts.daily ? charts.daily.title : 'Что было и что ожидается');
        setText('forecastBreakdownChartTitle', charts.breakdown ? charts.breakdown.title : 'Вероятность пожара по ближайшим дням');
        setText('forecastWeekdayChartTitle', charts.weekday ? charts.weekday.title : 'В какие дни недели пожары случаются чаще');

        var summaryNode = byId('forecastSummaryLine');
        if (summaryNode) {
            summaryNode.textContent =
                (summary.slice_label || 'Все пожары') +
                ' | Средняя вероятность: ' + (summary.average_probability_display || '0%') +
                ' | Максимум: ' + (summary.peak_forecast_probability_display || '0%') + ' (' + (summary.peak_forecast_day_display || '-') + ')' +
                ' | К последним 4 неделям: ' + (summary.forecast_vs_recent_display || '0%');
        }

        renderInsights(data.insights || []);
        renderNotes(data.notes || []);
        renderForecastTable(data.forecast_rows || []);
        renderChart(charts.daily, 'forecastDailyChart', 'forecastDailyChartFallback');
        renderChart(charts.breakdown, 'forecastBreakdownChart', 'forecastBreakdownChartFallback');
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
            var response = await fetch('/api/forecasting-data?' + query, { headers: { Accept: 'application/json' } });
            if (!response.ok) {
                throw new Error('fetch failed');
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

        if (window.__FIRE_FORECAST_INITIAL__) {
            applyForecastData(window.__FIRE_FORECAST_INITIAL__);
        }
    });
})();


