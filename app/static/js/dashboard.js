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

    function getSelectedText(selectNode, fallback) {
        if (!selectNode || !selectNode.options.length) {
            return fallback;
        }
        const option = selectNode.options[selectNode.selectedIndex];
        return option ? option.text : fallback;
    }

    function renderFilterSummary(labels) {
        const summaryNode = byId('filterSummary');
        if (!summaryNode) {
            return;
        }

        if (labels) {
            summaryNode.textContent = 'Таблица: ' + labels.table + ' | Год: ' + labels.year + ' | Разрез: ' + labels.group;
            return;
        }

        summaryNode.textContent = 'Таблица: ' + getSelectedText(byId('tableFilter'), 'Все таблицы') +
            ' | Год: ' + getSelectedText(byId('yearFilter'), 'Все годы') +
            ' | Разрез: ' + getSelectedText(byId('groupColumnFilter'), 'Категория риска');
    }

    function setText(id, value) {
        const node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
    }

    function setHtml(id, value) {
        const node = byId(id);
        if (node) {
            node.innerHTML = value;
        }
    }

    function setSelectOptions(selectId, options, selectedValue, emptyLabel) {
        const selectNode = byId(selectId);
        if (!selectNode) {
            return;
        }

        const safeOptions = Array.isArray(options) && options.length ? options : [{ value: '', label: emptyLabel }];
        let currentGroup = '';
        let html = '';

        safeOptions.forEach(function (option) {
            const optionGroup = option.group || '';
            if (optionGroup !== currentGroup) {
                if (currentGroup) {
                    html += '</optgroup>';
                }
                if (optionGroup) {
                    html += '<optgroup label="' + escapeHtml(optionGroup) + '">';
                }
                currentGroup = optionGroup;
            }

            const selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            html += '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        });

        if (currentGroup) {
            html += '</optgroup>';
        }

        selectNode.innerHTML = html;
    }

    function renderChartFallback(chart, container, containerId) {
        const message = chart && chart.empty_message
            ? chart.empty_message
            : 'Интерактивный график не загрузился.';
        container.innerHTML = '<div class="chart-empty">' + escapeHtml(message) + '</div>';
    }

    function renderPlotlyChart(chart, containerId) {
        const container = byId(containerId);
        if (!container) {
            return;
        }

        const figure = chart && chart.plotly;
        if (!figure || !window.Plotly) {
            renderChartFallback(chart, container, containerId);
            return;
        }

        try {
            container.innerHTML = '';
            window.Plotly.react(
                container,
                Array.isArray(figure.data) ? figure.data : [],
                figure.layout || {},
                figure.config || { responsive: true }
            );
        } catch (error) {
            console.error('Plotly render failed for', containerId, error);
            renderChartFallback(chart, container, containerId);
        }
    }

    function renderRankingList(containerId, items, emptyMessage, accentClass) {
        const container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<div class="ranking-row ' + accentClass + '" title="' + escapeHtml(item.label + ': ' + item.value_display) + '">' +
                '<div class="ranking-label">' + escapeHtml(item.label) + '</div>' +
                '<div class="ranking-meta">' + escapeHtml(item.value_display) + '</div>' +
            '</div>';
        }).join('');
    }

    function renderHighlights(items) {
        const container = byId('highlightsGrid');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Инсайты появятся, когда в базе будут данные.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="insight-card tone-' + escapeHtml(item.tone || 'muted') + '" title="' + escapeHtml(item.value) + '">' +
                '<span class="insight-label">' + escapeHtml(item.label) + '</span>' +
                '<strong class="insight-value">' + escapeHtml(item.value) + '</strong>' +
                '<span class="insight-meta">' + escapeHtml(item.meta) + '</span>' +
            '</article>';
        }).join('');
    }

    function renderNotes(notes) {
        const panel = byId('dashboardNotesPanel');
        const list = byId('dashboardNotesList');
        if (!panel || !list) {
            return;
        }

        if (!Array.isArray(notes) || !notes.length) {
            panel.classList.add('is-hidden');
            list.innerHTML = '';
            return;
        }

        panel.classList.remove('is-hidden');
        list.innerHTML = notes.map(function (note) {
            return '<li>' + escapeHtml(note) + '</li>';
        }).join('');
    }

    function applyDashboardData(data) {
        if (!data) {
            return;
        }

        const summary = data.summary || {};
        const scope = data.scope || {};
        const trend = data.trend || {};
        const charts = data.charts || {};
        const rankings = data.rankings || {};
        const widgets = data.widgets || {};
        const filters = data.filters || {};

        setSelectOptions('tableFilter', filters.available_tables, filters.table_name, 'Все таблицы');
        setSelectOptions('yearFilter', filters.available_years, filters.year, 'Все годы');
        setSelectOptions('groupColumnFilter', filters.available_group_columns, filters.group_column, 'Нет доступных колонок');

        setText('heroTableLabel', scope.table_label || 'Все таблицы');
        setText('heroYearLabel', scope.year_label || 'Все годы');
        setText('heroGroupLabel', scope.group_label || 'Нет данных');
        renderFilterSummary({
            table: scope.table_label || 'Все таблицы',
            year: scope.year_label || 'Все годы',
            group: scope.group_label || 'Нет данных'
        });

        setText('trendTitle', trend.title || 'Динамика последнего года');
        setText('trendCurrentValue', trend.current_value_display || '0');
        setText('trendCurrentYear', trend.current_year || '-');
        setText('trendDeltaValue', trend.delta_display || 'Нет базы сравнения');
        setText('trendDescription', trend.description || '');

        const trendCard = byId('trendCard');
        if (trendCard) {
            trendCard.classList.remove('trend-up', 'trend-down', 'trend-flat');
            trendCard.classList.add('trend-' + (trend.direction || 'flat'));
        }

        setText('coverageYearsValue', summary.years_covered_display || '0');
        setText('coveragePeriodValue', summary.period_label || 'Нет данных');
        setText('coverageFillRate', summary.area_fill_rate_display || '0%');

        setText('firesCountValue', summary.fires_count_display || '0');
        setText('firesCountFoot', summary.year_label || 'Все годы');
        setText('deathsValue', summary.deaths_display || '0');
        setText('injuriesValue', summary.injuries_display || '0');
        setText('evacuatedValue', summary.evacuated_display || '0');
        setText('childrenValue', summary.evacuated_children_display || '0');
        setText('rescuedValue', summary.rescued_children_display || '0');

        setText('sidebarDatabaseTablesCount', scope.database_tables_count_display || '0');
        setText('sidebarYearsCoveredCount', summary.years_covered_display || '0');
        setText('sidebarPeriodLabel', summary.period_label || 'Нет данных');

        setText('yearlyFiresTitle', charts.yearly_fires ? charts.yearly_fires.title : 'Причины возгораний');
        setText('distributionTitle', charts.distribution ? charts.distribution.title : 'Распределение: Категория риска');
        setText('yearlyAreaTitle', charts.yearly_area ? charts.yearly_area.title : 'Последствия, эвакуация и дети');
        setText('tableBreakdownTitle', charts.table_breakdown ? charts.table_breakdown.title : 'Эвакуация и дети');
        setText('monthlyProfileTitle', charts.monthly_profile ? charts.monthly_profile.title : 'Сезонность по месяцам');
        setText('areaBucketsTitle', charts.area_buckets ? charts.area_buckets.title : 'Структура по площади пожара');
        setText('sqlCausesTitle', widgets.causes ? widgets.causes.title : 'SQL-виджет: причины');
        setText('sqlDistrictsTitle', widgets.districts ? widgets.districts.title : 'SQL-виджет: районы');
        setText('sqlSeasonsTitle', widgets.seasons ? widgets.seasons.title : 'SQL-виджет: сезоны');

        renderPlotlyChart(charts.yearly_fires, 'yearlyFiresChart');
        renderPlotlyChart(charts.distribution, 'distributionChart');
        renderPlotlyChart(charts.yearly_area, 'yearlyAreaChart');
        renderPlotlyChart(charts.table_breakdown, 'tableBreakdownChart');
        renderPlotlyChart(charts.monthly_profile, 'monthlyProfileChart');
        renderPlotlyChart(charts.area_buckets, 'areaBucketsChart');
        renderPlotlyChart(widgets.causes, 'sqlCausesChart');
        renderPlotlyChart(widgets.districts, 'sqlDistrictsChart');
        renderPlotlyChart(widgets.seasons, 'sqlSeasonsChart');

        renderHighlights(data.highlights || []);
        renderRankingList('topDistributionList', rankings.top_distribution, 'Нет данных по распределению.', 'ranking-row-fire');
        renderRankingList('topTablesList', rankings.top_tables, 'Нет таблиц в текущем фильтре.', 'ranking-row-table');
        renderRankingList('recentYearsList', rankings.recent_years, 'Недостаточно годовых данных.', 'ranking-row-year');
        renderNotes(data.notes || []);
    }

    async function fetchDashboardData() {
        const form = byId('filtersForm');
        const button = byId('refreshDashboardButton');
        if (!form) {
            return;
        }

        const params = new URLSearchParams(new FormData(form));
        const query = params.toString();

        if (button) {
            button.disabled = true;
        }

        try {
            const response = await fetch('/api/dashboard-data?' + query, {
                headers: {
                    'Accept': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error('Не удалось обновить dashboard');
            }

            const data = await response.json();
            applyDashboardData(data);
            window.history.replaceState({}, '', query ? '/?' + query : '/');
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
        const form = byId('filtersForm');
        const tableSelect = byId('tableFilter');
        const yearSelect = byId('yearFilter');
        const groupColumnSelect = byId('groupColumnFilter');

        [tableSelect, yearSelect, groupColumnSelect].forEach(function (selectNode) {
            if (selectNode) {
                selectNode.addEventListener('change', function () {
                    renderFilterSummary();
                });
            }
        });

        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchDashboardData();
            });
        }


        const initialData = window.__FIRE_DASHBOARD_INITIAL_DATA__;
        if (initialData) {
            applyDashboardData(initialData);
        } else {
            renderFilterSummary();
        }

        window.fireDashboard = {
            reload: fetchDashboardData,
            afterImport: function () {
                fetchDashboardData();
            }
        };
    });
})();











