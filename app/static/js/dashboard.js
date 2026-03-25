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

    function setSelectOptions(selectId, options, selectedValue, emptyLabel) {
        const selectNode = byId(selectId);
        if (!selectNode) {
            return;
        }

        const selectedValues = Array.isArray(selectedValue)
            ? new Set(selectedValue.map(function (value) { return String(value); }))
            : new Set([String(selectedValue == null ? '' : selectedValue)]);
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

            const selected = selectedValues.has(String(option.value)) ? ' selected' : '';
            html += '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        });

        if (currentGroup) {
            html += '</optgroup>';
        }

        selectNode.innerHTML = html;
    }

    function renderChartFallback(chart, container) {
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
            renderChartFallback(chart, container);
            return;
        }

        const plotlyApi = window.Plotly;
        const renderChart = typeof plotlyApi.newPlot === 'function' ? plotlyApi.newPlot.bind(plotlyApi) : plotlyApi.react.bind(plotlyApi);
        const data = Array.isArray(figure.data) ? figure.data : [];
        const layout = figure.layout || {};
        const config = figure.config || { responsive: true };

        try {
            if (typeof plotlyApi.purge === 'function') {
                plotlyApi.purge(container);
            }
            container.innerHTML = '';
            const renderPromise = renderChart(container, data, layout, config);
            if (renderPromise && typeof renderPromise.catch === 'function') {
                renderPromise.catch(function (error) {
                    console.error('Plotly render failed for', containerId, error);
                    renderChartFallback(chart, container);
                });
            }
        } catch (error) {
            console.error('Plotly render failed for', containerId, error);
            renderChartFallback(chart, container);
        }
    }

    function applyToneClass(node, tone) {
        if (!node) {
            return;
        }

        node.className = node.className.replace(/\btone-[a-z]+\b/g, '').replace(/\s+/g, ' ').trim();
        if (tone) {
            node.className += (node.className ? ' ' : '') + 'tone-' + tone;
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

    function renderNotesPanel(notes) {
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

    function renderSimpleNotes(containerId, notes, emptyMessage) {
        const container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(notes) || !notes.length) {
            container.innerHTML = '<li>' + escapeHtml(emptyMessage) + '</li>';
            return;
        }

        container.innerHTML = notes.map(function (note) {
            return '<li>' + escapeHtml(note) + '</li>';
        }).join('');
    }

    function renderManagementCards(items) {
        const container = byId('managementBriefCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Сводка появится после загрузки данных.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="executive-brief-card tone-' + escapeHtml(item.tone || 'sky') + '">' +
                '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>' +
                '<strong class="stat-value executive-brief-value">' + escapeHtml(item.value || '-') + '</strong>' +
                '<span class="stat-foot">' + escapeHtml(item.meta || '') + '</span>' +
            '</article>';
        }).join('');
    }

    function renderManagementTerritories(items) {
        const container = byId('managementTerritories');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Территории первого внимания появятся после расчёта.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="executive-territory-card tone-' + escapeHtml(item.risk_tone || 'sky') + '">' +
                '<div class="executive-territory-head">' +
                    '<strong>' + escapeHtml(item.label || 'Территория') + '</strong>' +
                    '<span class="executive-territory-score">' + escapeHtml(item.risk_display || '0 / 100') + '</span>' +
                '</div>' +
                '<div class="executive-territory-tags">' +
                    '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.risk_tone || 'sky') + '">' + escapeHtml(item.risk_class_label || 'Нет оценки') + '</span>' +
                    '<span class="forecast-badge risk-badge tone-sky">' + escapeHtml(item.priority_label || 'Плановое наблюдение') + '</span>' +
                '</div>' +
                '<p class="executive-territory-reason">' + escapeHtml(item.drivers_display || 'Недостаточно данных для объяснения приоритета.') + '</p>' +
                '<div class="executive-territory-action">' +
                    '<strong>' + escapeHtml(item.action_label || 'Плановое наблюдение') + '</strong>' +
                    '<span>' + escapeHtml(item.action_hint || '') + '</span>' +
                '</div>' +
                '<div class="executive-territory-meta">' +
                    '<span>' + escapeHtml(item.context_label || 'Контекст не указан') + '</span>' +
                    '<span>Последний пожар: ' + escapeHtml(item.last_fire_display || '-') + '</span>' +
                '</div>' +
            '</article>';
        }).join('');
    }

    function renderManagementActions(items) {
        const container = byId('managementActionList');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Рекомендации появятся после расчёта.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="executive-action-item">' +
                '<strong>' + escapeHtml(item.label || 'Рекомендация') + '</strong>' +
                '<span>' + escapeHtml(item.detail || '') + '</span>' +
            '</article>';
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
        const filters = data.filters || {};
        const management = data.management || {};

        setSelectOptions('tableFilter', filters.available_tables, filters.table_name, 'Все таблицы');
        setSelectOptions('yearFilter', [{ value: 'all', label: 'Все годы' }].concat(filters.available_years || []), filters.year || 'all', 'Все годы');
        setSelectOptions('groupColumnFilter', filters.available_group_columns, filters.group_column, 'Нет доступных колонок');

        setText('heroTableLabel', scope.table_label || 'Все таблицы');
        setText('heroYearLabel', scope.year_label || 'Все годы');
        setText('heroGroupLabel', scope.group_label || 'Нет данных');
        setText('dashboardLeadSummary', management.summary_line || 'После загрузки данных здесь появится управленческая сводка.');
        setText('managementHeroPriority', management.priority_territory_label || '-');
        setText('managementHeroPriorityMeta', management.priority_reason || 'Недостаточно данных для определения первого приоритета.');
        setText('managementHeroConfidence', management.confidence_label || 'Ограниченная');
        setText('managementHeroConfidenceScore', management.confidence_score_display || '0 / 100');
        setText('managementHeroConfidenceMeta', management.confidence_summary || 'После загрузки данных здесь появится уровень доверия к сводке.');
        renderFilterSummary({
            table: scope.table_label || 'Все таблицы',
            year: scope.year_label || 'Все годы',
            group: scope.group_label || 'Нет данных'
        });

        applyToneClass(byId('dashboardPriorityCard'), management.priority_tone || 'sky');
        applyToneClass(byId('dashboardConfidenceCard'), management.confidence_tone || 'fire');
        renderManagementCards(management.brief_cards || []);
        renderManagementTerritories(management.territories || []);
        renderManagementActions(management.actions || []);
        renderSimpleNotes('managementNotesList', management.notes || [], 'Ограничения и примечания появятся после загрузки данных.');

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

        setText('firesCountValue', summary.fires_count_display || '0');
        setText('firesCountFoot', scope.table_label || 'Все таблицы');
        setText('deathsValue', summary.deaths_display || '0');
        setText('injuriesValue', summary.injuries_display || '0');
        setText('evacuatedValue', summary.evacuated_display || '0');
        setText('childrenValue', summary.evacuated_children_display || '0');
        setText('rescuedValue', summary.rescued_children_display || '0');

        setText('sidebarDatabaseTablesCount', scope.database_tables_count_display || '0');
        setText('sidebarYearsCoveredCount', summary.years_covered_display || '0');
        setText('sidebarPeriodLabel', summary.period_label || 'Нет данных');

        setText('yearlyFiresTitle', charts.yearly_fires ? charts.yearly_fires.title : 'Причины возгораний');
        setText('distributionTitle', charts.distribution ? charts.distribution.title : 'Распределение по выбранному разрезу');
        setText('yearlyAreaTitle', charts.yearly_area ? charts.yearly_area.title : 'Последствия пожара');
        setText('monthlyProfileTitle', charts.monthly_profile ? charts.monthly_profile.title : 'Сезонность по месяцам');
        setText('areaBucketsTitle', charts.area_buckets ? charts.area_buckets.title : 'Структура по площади пожара');
        setText('distributionMeta', charts.distribution ? charts.distribution.description : 'Распределение по выбранному разрезу.');
        setText('yearlyAreaMeta', charts.yearly_area ? charts.yearly_area.description : 'Последствия пожара, эвакуация и влияние на население.');
        setText('monthlyProfileMeta', charts.monthly_profile ? charts.monthly_profile.description : 'Сезонная динамика пожаров по месяцам.');
        setText('areaBucketsMeta', charts.area_buckets ? charts.area_buckets.description : 'Распределение по диапазонам площади пожара.');

        renderPlotlyChart(charts.yearly_fires, 'yearlyFiresChart');
        renderPlotlyChart(charts.distribution, 'distributionChart');
        renderPlotlyChart(charts.yearly_area, 'yearlyAreaChart');
        renderPlotlyChart(charts.monthly_profile, 'monthlyProfileChart');
        renderPlotlyChart(charts.area_buckets, 'areaBucketsChart');

        renderRankingList('topDistributionList', rankings.top_distribution, 'Нет данных по распределению.', 'ranking-row-fire');
        renderRankingList('topTablesList', rankings.top_tables, 'Нет таблиц в текущем фильтре.', 'ranking-row-table');
        renderRankingList('recentYearsList', rankings.recent_years, 'Недостаточно годовых данных.', 'ranking-row-year');
        renderNotesPanel(data.notes || []);
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
                throw new Error('Не удалось обновить панель');
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
        const shouldFetchOnLoad = !initialData || initialData.bootstrap_mode === 'deferred';
        if (initialData) {
            applyDashboardData(initialData);
        } else {
            renderFilterSummary();
        }

        if (shouldFetchOnLoad) {
            fetchDashboardData();
        }

        window.fireDashboard = {
            reload: fetchDashboardData,
            afterImport: function () {
                fetchDashboardData();
            }
        };
    });
})();
