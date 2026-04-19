(function (global) {
    var shared = global.FireUi;
    if (!shared) {
        return;
    }

    global.DashboardRender = {
        create: function createDashboardRender() {
            var applyToneClass = shared.applyToneClass;
            var byId = shared.byId;
            var escapeHtml = shared.escapeHtml;
            var renderListItems = shared.renderListItems;
            var renderPlotlyInContainer = shared.renderPlotlyInContainer;
            var setHref = shared.setHref;
            var setSelectOptions = shared.setSelectOptions;
            var setText = shared.setText;

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

    function renderDashboardCharts(charts) {
        var safeCharts = charts || {};
        renderPlotlyInContainer(safeCharts.yearly_fires, 'yearlyFiresChart');
        renderPlotlyInContainer(safeCharts.distribution, 'distributionChart');
        renderPlotlyInContainer(safeCharts.yearly_area, 'yearlyAreaChart');
        renderPlotlyInContainer(safeCharts.cumulative_area, 'cumulativeAreaChart');
        renderPlotlyInContainer(safeCharts.monthly_heatmap, 'monthlyHeatmapChart');
        renderPlotlyInContainer(safeCharts.monthly_profile, 'monthlyProfileChart');
        renderPlotlyInContainer(safeCharts.area_buckets, 'areaBucketsChart');
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
        renderListItems('dashboardNotesList', notes, '');
    }

    function hideDashboardError() {
        const card = byId('dashboardInlineError');
        if (card) {
            card.classList.add('is-hidden');
        }
        setText('dashboardInlineErrorLead', '');
        setText('dashboardInlineErrorMessage', '');
    }

    function showDashboardError(error) {
        const card = byId('dashboardInlineError');
        if (!card) {
            return;
        }

        const statusCode = Number(error && error.dashboardStatusCode ? error.dashboardStatusCode : 0);
        const errorId = error && error.dashboardErrorId ? String(error.dashboardErrorId) : '';
        const baseMessage = error && error.message
            ? String(error.message)
            : 'РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ РїР°РЅРµР»СЊ. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ.';
        const lead = statusCode >= 500
            ? 'РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ РїР°РЅРµР»СЊ'
            : statusCode >= 400
                ? 'РџСЂРѕРІРµСЂСЊС‚Рµ РїР°СЂР°РјРµС‚СЂС‹ Р·Р°РїСЂРѕСЃР°'
                : 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РґР°РЅРЅС‹Рµ';
        const fullMessage = errorId ? baseMessage + ' РљРѕРґ РѕС€РёР±РєРё: ' + errorId + '.' : baseMessage;

        setText('dashboardInlineErrorLead', lead);
        setText('dashboardInlineErrorMessage', fullMessage);
        card.classList.remove('is-hidden');
    }

function renderManagementCards(items) {
        const container = byId('managementBriefCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РЎРІРѕРґРєР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё РґР°РЅРЅС‹С….</div>';
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
            container.innerHTML = '<div class="mini-empty">РўРµСЂСЂРёС‚РѕСЂРёРё РїРµСЂРІРѕРіРѕ РІРЅРёРјР°РЅРёСЏ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="executive-territory-card tone-' + escapeHtml(item.risk_tone || 'sky') + '">' +
                '<div class="executive-territory-head">' +
                    '<strong>' + escapeHtml(item.label || 'РўРµСЂСЂРёС‚РѕСЂРёСЏ') + '</strong>' +
                    '<span class="executive-territory-score">' + escapeHtml(item.risk_display || '0 / 100') + '</span>' +
                '</div>' +
                '<div class="executive-territory-tags">' +
                    '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.risk_tone || 'sky') + '">' + escapeHtml(item.risk_class_label || 'РќРµС‚ РѕС†РµРЅРєРё') + '</span>' +
                    '<span class="forecast-badge risk-badge tone-sky">' + escapeHtml(item.priority_label || 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ') + '</span>' +
                '</div>' +
                '<p class="executive-territory-reason">' + escapeHtml(item.drivers_display || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ РїСЂРёРѕСЂРёС‚РµС‚Р°.') + '</p>' +
                '<div class="executive-territory-action">' +
                    '<strong>' + escapeHtml(item.action_label || 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ') + '</strong>' +
                    '<span>' + escapeHtml(item.action_hint || '') + '</span>' +
                '</div>' +
                '<div class="executive-territory-meta">' +
                    '<span>' + escapeHtml(item.context_label || 'РљРѕРЅС‚РµРєСЃС‚ РЅРµ СѓРєР°Р·Р°РЅ') + '</span>' +
                    '<span>РџРѕСЃР»РµРґРЅРёР№ РїРѕР¶Р°СЂ: ' + escapeHtml(item.last_fire_display || '-') + '</span>' +
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
            container.innerHTML = '<div class="mini-empty">Р РµРєРѕРјРµРЅРґР°С†РёРё РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="executive-action-item">' +
                '<strong>' + escapeHtml(item.label || 'Р РµРєРѕРјРµРЅРґР°С†РёСЏ') + '</strong>' +
                '<span>' + escapeHtml(item.detail || '') + '</span>' +
            '</article>';
        }).join('');
    }

    function buildDashboardBriefHref(filters) {
        const params = new URLSearchParams();
        const safeFilters = filters || {};

        if (safeFilters.table_name) {
            params.set('table_name', safeFilters.table_name);
        }
        if (safeFilters.year) {
            params.set('year', safeFilters.year);
        }
        if (safeFilters.group_column) {
            params.set('group_column', safeFilters.group_column);
        }
        if (safeFilters.horizon_days) {
            params.set('horizon_days', safeFilters.horizon_days);
        }

        const query = params.toString();
        return '/brief/dashboard.txt' + (query ? '?' + query : '');
    }

    function updateDashboardBriefExport(filters) {
        const href = buildDashboardBriefHref(filters || {});
        Array.prototype.forEach.call(
            document.querySelectorAll('#dashboardBrief .executive-brief-download, #dashboardBrief .executive-brief-summary-action'),
            function (link) {
                link.setAttribute('href', href);
            }
        );
    }

    function buildDashboardScreenHref(path, filters, hash) {
        const params = new URLSearchParams();
        const safeFilters = filters || {};

        if (safeFilters.table_name && safeFilters.table_name !== 'all') {
            params.set('table_name', safeFilters.table_name);
        }

        const query = params.toString();
        return path + (query ? '?' + query : '') + (hash || '');
    }

    function updateDashboardScreenLinks(filters) {
        const safeFilters = filters || {};
        setHref('dashboardScenarioLink', buildDashboardScreenHref('/forecasting', safeFilters, ''));
        setHref('dashboardMlLink', buildDashboardScreenHref('/ml-model', safeFilters, ''));
        setHref('dashboardDecisionLink', buildDashboardScreenHref('/forecasting', safeFilters, '#forecastDetails'));
    }

    function buildDashboardPageHref(filters, mode) {
        const params = new URLSearchParams();
        const safeFilters = filters || {};

        if (safeFilters.table_name) {
            params.set('table_name', safeFilters.table_name);
        }
        if (safeFilters.year) {
            params.set('year', safeFilters.year);
        }
        if (safeFilters.group_column) {
            params.set('group_column', safeFilters.group_column);
        }
        if (safeFilters.horizon_days) {
            params.set('horizon_days', safeFilters.horizon_days);
        }
        if (mode) {
            params.set('mode', mode);
        }

        const query = params.toString();
        return query ? '/?' + query : '/';
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
        const brief = management.brief || {};

        setSelectOptions('tableFilter', filters.available_tables, filters.table_name, 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹');
        setSelectOptions('yearFilter', [{ value: 'all', label: 'Р’СЃРµ РіРѕРґС‹' }].concat(filters.available_years || []), filters.year || 'all', 'Р’СЃРµ РіРѕРґС‹');
        setSelectOptions('groupColumnFilter', filters.available_group_columns, filters.group_column, 'РќРµС‚ РґРѕСЃС‚СѓРїРЅС‹С… РєРѕР»РѕРЅРѕРє');
        setSelectOptions('horizonDaysFilter', filters.available_horizon_days, filters.horizon_days || '14', '14 дней');

        setText('heroTableLabel', scope.table_label || 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹');
        setText('heroYearLabel', scope.year_label || 'Р’СЃРµ РіРѕРґС‹');
        setText('heroGroupLabel', scope.group_label || 'РќРµС‚ РґР°РЅРЅС‹С…');
        setText('heroHorizonDays', String(management.priority_horizon_days || filters.horizon_days || '14') + ' дней');
        setText('dashboardLeadSummary', brief.lead || management.summary_line || 'РџРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё РґР°РЅРЅС‹С… Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅС‹Р№ РІС‹РІРѕРґ Рё РїРµСЂРІР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РґР»СЏ РїСЂРѕРІРµСЂРєРё.');
        setText('managementHeroPriority', brief.top_territory_label || management.priority_territory_label || '-');
        setText('managementHeroPriorityMeta', brief.priority_reason || management.priority_reason || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕРїСЂРµРґРµР»РµРЅРёСЏ РїРµСЂРІРѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё.');
        setText('managementHeroConfidence', brief.confidence_label || management.confidence_label || 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ');
        setText('managementHeroConfidenceScore', brief.confidence_score_display || management.confidence_score_display || '0 / 100');
        setText('managementHeroConfidenceMeta', brief.confidence_summary || management.confidence_summary || 'РџРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё РґР°РЅРЅС‹С… Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЏСЃРЅРµРЅРёРµ, РЅР°СЃРєРѕР»СЊРєРѕ РЅР°РґРµР¶РµРЅ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅС‹Р№ РїСЂРёРѕСЂРёС‚РµС‚.');
        setText('dashboardExportBriefExcerpt', brief.export_excerpt || management.export_excerpt || 'РљСЂР°С‚РєР°СЏ СЌРєСЃРїРѕСЂС‚РёСЂСѓРµРјР°СЏ СЃРїСЂР°РІРєР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё РґР°РЅРЅС‹С….');
        applyToneClass(byId('dashboardPriorityCard'), brief.priority_tone || management.priority_tone || 'sky');
        applyToneClass(byId('dashboardConfidenceCard'), brief.confidence_tone || management.confidence_tone || 'fire');
        renderManagementCards(brief.cards || management.brief_cards || []);
        renderManagementTerritories(management.territories || []);
        renderManagementActions(management.actions || []);
        renderListItems('managementNotesList', brief.notes || management.notes || [], 'РћРіСЂР°РЅРёС‡РµРЅРёСЏ Рё РїСЂРёРјРµС‡Р°РЅРёСЏ Рє С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРјСѓ РІС‹РІРѕРґСѓ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё РґР°РЅРЅС‹С….');
        updateDashboardBriefExport({
            table_name: filters.table_name || '',
            year: filters.year || 'all',
            group_column: filters.group_column || '',
            horizon_days: filters.horizon_days || '14'
        });
        updateDashboardScreenLinks({
            table_name: filters.table_name || ''
        });

        setText('trendTitle', trend.title || 'РљР°Рє РјРµРЅСЏР»Р°СЃСЊ СЃРёС‚СѓР°С†РёСЏ');
        setText('trendCurrentValue', trend.current_value_display || '0');
        setText('trendCurrentYear', trend.current_year || '-');
        setText('trendDeltaValue', trend.delta_display || 'РќРµС‚ Р±Р°Р·С‹ СЃСЂР°РІРЅРµРЅРёСЏ');
        setText('trendDescription', trend.description || '');

        const trendCard = byId('trendCard');
        if (trendCard) {
            trendCard.classList.remove('trend-up', 'trend-down', 'trend-flat');
            trendCard.classList.add('trend-' + (trend.direction || 'flat'));
        }

        setText('firesCountValue', summary.fires_count_display || '0');
        setText('firesCountFoot', scope.table_label || 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹');
        setText('deathsValue', summary.deaths_display || '0');
        setText('injuriesValue', summary.injuries_display || '0');
        setText('evacuatedValue', summary.evacuated_display || '0');
        setText('childrenTotalValue', summary.children_total_display || '0');
        setText('severityValue', (summary.lethality_rate_display || '0.0') + ' РЅР° 100 РїРѕР¶Р°СЂРѕРІ');

        setText('sidebarDatabaseTablesCount', scope.database_tables_count_display || '0');
        setText('sidebarYearsCoveredCount', summary.years_covered_display || '0');
        setText('sidebarPeriodLabel', summary.period_label || 'РќРµС‚ РґР°РЅРЅС‹С…');

        setText('yearlyFiresTitle', charts.yearly_fires ? charts.yearly_fires.title : 'РџСЂРёС‡РёРЅС‹ РІРѕР·РіРѕСЂР°РЅРёР№');
        setText('distributionTitle', charts.distribution ? charts.distribution.title : 'Р Р°СЃРїСЂРµРґРµР»РµРЅРёРµ РїРѕ РІС‹Р±СЂР°РЅРЅРѕРјСѓ СЂР°Р·СЂРµР·Сѓ');
        setText('yearlyAreaTitle', charts.yearly_area ? charts.yearly_area.title : 'РџРѕСЃР»РµРґСЃС‚РІРёСЏ РїРѕР¶Р°СЂР°');

        setText('cumulativeAreaTitle', charts.cumulative_area ? charts.cumulative_area.title : 'РќР°РєРѕРїР»РµРЅРЅР°СЏ РїР»РѕС‰Р°РґСЊ РїРѕ РґРЅСЏРј РіРѕРґР°');
        setText('monthlyHeatmapTitle', charts.monthly_heatmap ? charts.monthly_heatmap.title : 'РЎРµР·РѕРЅРЅРѕСЃС‚СЊ РїРѕ РјРµСЃСЏС†Р°Рј Рё РіРѕРґР°Рј');
        setText('monthlyProfileTitle', charts.monthly_profile ? charts.monthly_profile.title : 'РЎРµР·РѕРЅРЅРѕСЃС‚СЊ РїРѕ РјРµСЃСЏС†Р°Рј');
        setText('areaBucketsTitle', charts.area_buckets ? charts.area_buckets.title : 'РЎС‚СЂСѓРєС‚СѓСЂР° РїРѕ РїР»РѕС‰Р°РґРё РїРѕР¶Р°СЂР°');
        setText('distributionMeta', charts.distribution ? charts.distribution.description : 'Р§С‚Рѕ РїРѕРєР°Р·С‹РІР°РµС‚ Р±Р»РѕРє: РєР°Рє РїРѕР¶Р°СЂС‹ СЂР°СЃРїСЂРµРґРµР»СЏСЋС‚СЃСЏ РїРѕ РІС‹Р±СЂР°РЅРЅРѕР№ РіСЂСѓРїРїРµ.');
        setText('yearlyAreaMeta', charts.yearly_area ? charts.yearly_area.description : 'Р§С‚Рѕ РїРѕРєР°Р·С‹РІР°РµС‚ Р±Р»РѕРє: С‚СЏР¶РµСЃС‚СЊ РїРѕСЃР»РµРґСЃС‚РІРёР№ Рё РІР»РёСЏРЅРёРµ РїРѕР¶Р°СЂРѕРІ РЅР° Р»СЋРґРµР№.');

        setText('cumulativeAreaMeta', charts.cumulative_area ? charts.cumulative_area.description : 'РќР°РєРѕРїР»РµРЅРЅР°СЏ РїР»РѕС‰Р°РґСЊ: С‚РµРєСѓС‰РёР№ РіРѕРґ РїСЂРѕС‚РёРІ РїСЂРµРґС‹РґСѓС‰РµРіРѕ.');
        setText('monthlyHeatmapMeta', charts.monthly_heatmap ? charts.monthly_heatmap.description : 'РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР¶Р°СЂРѕРІ РїРѕ РјРµСЃСЏС†Р°Рј Рё РіРѕРґР°Рј.');
        setText('monthlyProfileMeta', charts.monthly_profile ? charts.monthly_profile.description : 'Р§С‚Рѕ РїРѕРєР°Р·С‹РІР°РµС‚ Р±Р»РѕРє: СЃРµР·РѕРЅРЅС‹Р№ СЂРёСЃСѓРЅРѕРє РїРѕР¶Р°СЂРѕРІ, РµСЃР»Рё РЅСѓР¶РЅРѕ РїР»Р°РЅРёСЂРѕРІР°С‚СЊ РїСЂРѕС„РёР»Р°РєС‚РёРєСѓ Р·Р°СЂР°РЅРµРµ.');
        setText('areaBucketsMeta', charts.area_buckets ? charts.area_buckets.description : 'Р§С‚Рѕ РїРѕРєР°Р·С‹РІР°РµС‚ Р±Р»РѕРє: РїСЂРµРѕР±Р»Р°РґР°СЋС‚ Р»Рё РЅРµР±РѕР»СЊС€РёРµ РёР»Рё РєСЂСѓРїРЅС‹Рµ РїРѕР¶Р°СЂС‹.');

        renderDashboardCharts(charts);

        renderRankingList('topDistributionList', rankings.top_distribution, 'РќРµС‚ РґР°РЅРЅС‹С… РїРѕ СЂР°СЃРїСЂРµРґРµР»РµРЅРёСЋ.', 'ranking-row-fire');
        renderRankingList('topTablesList', rankings.top_tables, 'РќРµС‚ С‚Р°Р±Р»РёС† РІ С‚РµРєСѓС‰РµРј С„РёР»СЊС‚СЂРµ.', 'ranking-row-table');
        renderRankingList('recentYearsList', rankings.recent_years, 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РіРѕРґРѕРІС‹С… РґР°РЅРЅС‹С….', 'ranking-row-year');
        renderNotesPanel(data.notes || []);
        if (shared.revealPageContent) { shared.revealPageContent(); }
    }

            return {
                applyDashboardData: applyDashboardData,
                buildDashboardPageHref: buildDashboardPageHref,
                hideDashboardError: hideDashboardError,
                renderDashboardCharts: renderDashboardCharts,
                showDashboardError: showDashboardError,
                updateDashboardBriefExport: updateDashboardBriefExport,
                updateDashboardScreenLinks: updateDashboardScreenLinks
            };
        }
    };
}(window));

