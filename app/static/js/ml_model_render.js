(function (global) {
    var shared = global.FireUi || {};
    var api = global.MlModelApi || {};
    var charts = global.MlModelCharts || {};

    var byId = shared.byId;
    var createTimerGroup = shared.createTimerGroup;
    var escapeHtml = shared.escapeHtml;
    var renderMetricCards = shared.renderMetricCards;
    var runProgressSequence = shared.runProgressSequence;
    var setHref = shared.setHref;
    var setSectionHidden = shared.setSectionHidden;
    var setSelectOptions = shared.setSelectOptions;
    var setStepProgress = shared.setStepProgress;
    var setText = shared.setText;
    var setValue = shared.setValue;

    var currentMlData = null;
    var progressTimers = createTimerGroup();
    var progressSteps = [
        {
            label: 'Р—Р°РіСЂСѓР·РєР° РґР°РЅРЅС‹С…',
            lead: 'Р—Р°РіСЂСѓР¶Р°РµРј РґР°РЅРЅС‹Рµ ML-РїСЂРѕРіРЅРѕР·Р°',
            message: 'РџРѕР»СѓС‡Р°РµРј РІС‹Р±СЂР°РЅРЅС‹Р№ СЃСЂРµР· Рё РѕР±РЅРѕРІР»СЏРµРј РїР°СЂР°РјРµС‚СЂС‹ СЃС‚СЂР°РЅРёС†С‹.'
        },
        {
            label: 'РђРіСЂРµРіР°С†РёСЏ',
            lead: 'РђРіСЂРµРіРёСЂСѓРµРј РёСЃС‚РѕСЂРёСЋ',
            message: 'РЎРѕР±РёСЂР°РµРј РґРЅРµРІРЅРѕР№ СЂСЏРґ, С„РёР»СЊС‚СЂС‹ Рё РґРѕСЃС‚СѓРїРЅС‹Рµ РїСЂРёР·РЅР°РєРё.'
        },
        {
            label: 'РћР±СѓС‡РµРЅРёРµ / РІР°Р»РёРґР°С†РёСЏ',
            lead: 'РћР±СѓС‡РµРЅРёРµ Рё РІР°Р»РёРґР°С†РёСЏ',
            message: 'РЎС‡РёС‚Р°РµРј backtesting, РїСЂРѕРіРЅРѕР· Рё РёС‚РѕРіРѕРІС‹Рµ С‚Р°Р±Р»РёС†С‹.'
        },
        {
            label: 'РџРѕСЃС‚СЂРѕРµРЅРёРµ РІРёР·СѓР°Р»РёР·Р°С†РёР№',
            lead: 'РћР±РЅРѕРІР»СЏРµРј РІРёР·СѓР°Р»РёР·Р°С†РёРё',
            message: 'РџРѕРґСЃС‚Р°РІР»СЏРµРј РіСЂР°С„РёРєРё, С‚Р°Р±Р»РёС†С‹ Рё РєР°СЂС‚РѕС‡РєРё СЂРµР·СѓР»СЊС‚Р°С‚Р°.'
        }
    ];

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

        var badgeLabel = 'РќСѓР¶РЅРѕ СѓС‚РѕС‡РЅРёС‚СЊ С„РёР»СЊС‚СЂС‹';
        if (data && data.error_message) {
            badgeLabel = 'РўСЂРµР±СѓРµС‚СЃСЏ РїРѕРІС‚РѕСЂРЅС‹Р№ СЂР°СЃС‡РµС‚';
        } else if ((api.isFetching && api.isFetching()) || (data && data.bootstrap_mode === 'deferred')) {
            badgeLabel = 'РЎРѕР±РёСЂР°РµРј ML-РїСЂРѕРіРЅРѕР·';
        } else if (data && data.has_data) {
            badgeLabel = 'ML-РїСЂРѕРіРЅРѕР· РіРѕС‚РѕРІ';
        }

        container.innerHTML = ''
            + '<span class="' + badgeClass + '">' + escapeHtml(badgeLabel) + '</span>'
            + '<div class="status-line"><span>РњРѕРґРµР»СЊ РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ</span><strong>' + escapeHtml(summary.count_model_label || 'Р РµРіСЂРµСЃСЃРёСЏ РџСѓР°СЃСЃРѕРЅР°') + '</strong></div>'
            + '<div class="status-line"><span>РЎРѕР±С‹С‚РёРµ РїРѕР¶Р°СЂР°</span><strong>' + escapeHtml(summary.event_model_label || 'РќРµ РѕР±СѓС‡РµРЅ') + '</strong></div>'
            + '<div class="status-line"><span>РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё</span><strong>' + escapeHtml(summary.backtest_method_label || 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё РЅРµ РІС‹РїРѕР»РЅРµРЅР°') + '</strong></div>'
            + '<div class="status-line"><span>РџРµСЂРёРѕРґ</span><strong>' + escapeHtml(summary.history_period_label || 'РќРµС‚ РґР°РЅРЅС‹С…') + '</strong></div>';
    }

    function renderHero(data) {
        var summary = data.summary || {};
        setText('mlModelDescription', summary.hero_summary || data.model_description || 'РџРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ РІС‹РІРѕРґ РїРѕ РѕР¶РёРґР°РµРјРѕРјСѓ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ Рё РЅР°РґРµР¶РЅРѕСЃС‚Рё СЂР°СЃС‡РµС‚Р°.');

        var heroTags = byId('mlHeroTags');
        if (heroTags) {
            heroTags.innerHTML = ''
                + '<span class="hero-tag">РўР°Р±Р»РёС†Р°: <strong>' + escapeHtml(summary.selected_table_label || 'РќРµС‚ С‚Р°Р±Р»РёС†С‹') + '</strong></span>'
                + '<span class="hero-tag">РСЃС‚РѕСЂРёСЏ РґР»СЏ СЂР°СЃС‡С‘С‚Р°: <strong>' + escapeHtml(summary.history_window_label || 'Р’СЃРµ РіРѕРґС‹') + '</strong></span>'
                + '<span class="hero-tag">Р“Р»Р°РІРЅС‹Р№ С„Р°РєС‚РѕСЂ РјРѕРґРµР»Рё: <strong>' + escapeHtml(summary.top_feature_label || '-') + '</strong></span>'
                + '<span class="hero-tag">РўРµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ СЃС†РµРЅР°СЂРёР№: <strong>' + escapeHtml(summary.temperature_scenario_display || 'РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ С‚РµРјРїРµСЂР°С‚СѓСЂР°') + '</strong></span>'
                + '<span class="hero-tag">'
                + (summary.event_probability_enabled
                    ? 'РЎСЂРµРґРЅСЏСЏ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ P(>=1 РїРѕР¶Р°СЂР°): <strong>' + escapeHtml(summary.average_event_probability_display || 'вЂ”') + '</strong>'
                    : 'РЎРѕР±С‹С‚РёРµ РїРѕР¶Р°СЂР°: <strong>РЅРµ РїРѕРєР°Р·Р°РЅРѕ</strong>')
                + '</span>';
        }

        var heroStats = byId('mlHeroStats');
        if (heroStats) {
            heroStats.innerHTML = ''
                + '<article class="hero-stat-card">'
                + '<span class="hero-stat-label">РЎСЂРµРґРЅРёР№ РѕР¶РёРґР°РµРјС‹Р№ РґРµРЅСЊ</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.average_expected_count_display || '0') + '</strong>'
                + '<span class="hero-stat-foot">РЎСЂРµРґРЅСЏСЏ РґРЅРµРІРЅР°СЏ РёРЅС‚РµРЅСЃРёРІРЅРѕСЃС‚СЊ РЅР° РІС‹Р±СЂР°РЅРЅРѕРј РіРѕСЂРёР·РѕРЅС‚Рµ РїСЂРѕРіРЅРѕР·Р°.</span>'
                + '</article>'
                + '<article class="hero-stat-card hero-stat-card-soft">'
                + '<span class="hero-stat-label">Р”РµРЅСЊ СЃ РјР°РєСЃРёРјР°Р»СЊРЅРѕР№ РЅР°РіСЂСѓР·РєРѕР№</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.peak_expected_count_display || '0') + '</strong>'
                + '<span class="hero-stat-foot">РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РѕР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ: ' + escapeHtml(summary.peak_expected_count_day_display || '-') + '.</span>'
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
            + '<span class="stat-label">РџРѕР¶Р°СЂРѕРІ РІ РѕР±СѓС‡РµРЅРёРё</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.fires_count_display || '0') + '</strong>'
            + '<span class="stat-foot">РџРѕСЃР»Рµ РІС‹Р±СЂР°РЅРЅС‹С… С„РёР»СЊС‚СЂРѕРІ.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Р”Р»РёРЅР° РёСЃС‚РѕСЂРёРё</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.history_days_display || '0') + '</strong>'
            + '<span class="stat-foot">РќРµРїСЂРµСЂС‹РІРЅС‹Р№ РґРЅРµРІРЅРѕР№ СЂСЏРґ СЃ РЅСѓР»СЏРјРё РјРµР¶РґСѓ РїРѕР¶Р°СЂР°РјРё.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">РћР¶РёРґР°РµРјРѕ РЅР° РІСЃС‘Рј РіРѕСЂРёР·РѕРЅС‚Рµ</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.predicted_total_display || '0') + '</strong>'
            + '<span class="stat-foot">РћР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РЅР° РІСЃРµРј РіРѕСЂРёР·РѕРЅС‚Рµ.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Р”РЅРµР№ СЃ РїРѕРІС‹С€РµРЅРЅРѕР№ РЅР°РіСЂСѓР·РєРѕР№</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.elevated_risk_days_display || '0') + '</strong>'
            + '<span class="stat-foot">РљРѕР»РёС‡РµСЃС‚РІРѕ РґРЅРµР№, РіРґРµ СЂРёСЃРє-РёРЅРґРµРєСЃ РЅРµ РЅРёР¶Рµ 75/100.</span>'
            + '</article>';
    }

    function renderOptionalMetricCards(sectionId, containerId, items, emptyMessage) {
        var hasItems = Array.isArray(items) && items.length;
        var container = byId(containerId);
        setSectionHidden(sectionId, !hasItems);
        if (!container) {
            return;
        }
        if (!hasItems) {
            container.innerHTML = '';
            return;
        }
        renderMetricCards(containerId, items, emptyMessage || '');
    }

    function renderIntervalCoverage(card) {
        var safeCard = card || {};
        setText('mlIntervalCoverageTitle', safeCard.label || 'РџРѕРєСЂС‹С‚РёРµ РёРЅС‚РµСЂРІР°Р»Р° РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С…');
        setText('mlIntervalCoverageValue', safeCard.value || 'вЂ”');
        setText(
            'mlIntervalCoverageMeta',
            safeCard.meta || 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РїСЂРѕРІРµСЂРєР° С‚РѕРіРѕ, РєР°Рє С‡Р°СЃС‚Рѕ С„Р°РєС‚РёС‡РµСЃРєРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РїРѕРїР°РґР°Р»Рѕ РІ РїСЂРѕРіРЅРѕР·РЅС‹Р№ РёРЅС‚РµСЂРІР°Р».'
        );
    }

    function renderImportanceNote(note) {
        var node = byId('mlImportanceChartNote');
        if (!node) {
            return;
        }
        node.textContent = note || '';
        node.classList.toggle('is-hidden', !note);
    }

    function renderCriticalNotes(items) {
        var panel = byId('mlNotesPanel');
        var container = byId('mlNotesList');
        var notes = Array.isArray(items)
            ? items.filter(function (item) { return item != null && String(item).trim(); }).slice(0, 2)
            : [];
        if (!panel || !container) {
            return;
        }

        panel.classList.toggle('is-hidden', !notes.length);
        container.innerHTML = notes.map(function (item) {
            return '<li>' + escapeHtml(item) + '</li>';
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
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(safeTable.empty_message || 'РЎСЂР°РІРЅРµРЅРёРµ baseline, СЃС†РµРЅР°СЂРЅРѕРіРѕ РїСЂРѕРіРЅРѕР·Р° Рё count-РјРѕРґРµР»РµР№ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё РЅР° РёСЃС‚РѕСЂРёРё.') + '</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="forecast-table">'
            + '<thead><tr><th>РњРµС‚РѕРґ</th><th>Р РѕР»СЊ</th><th>MAE</th><th>RMSE</th><th>SMAPE</th><th>Р”РµРІРёР°С†РёСЏ РџСѓР°СЃСЃРѕРЅР°</th><th>MAE Рє Р±Р°Р·РѕРІРѕР№ РјРѕРґРµР»Рё</th><th>РЎС‚Р°С‚СѓСЃ</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="РњРµС‚РѕРґ">' + escapeHtml(row.method_label || '-') + '</td>'
                    + '<td data-label="Р РѕР»СЊ">' + escapeHtml(row.role_label || '-') + '</td>'
                    + '<td data-label="MAE">' + escapeHtml(row.mae_display || '-') + '</td>'
                    + '<td data-label="RMSE">' + escapeHtml(row.rmse_display || '-') + '</td>'
                    + '<td data-label="SMAPE">' + escapeHtml(row.smape_display || '-') + '</td>'
                    + '<td data-label="Р”РµРІРёР°С†РёСЏ РџСѓР°СЃСЃРѕРЅР°">' + escapeHtml(row.poisson_display || '-') + '</td>'
                    + '<td data-label="MAE Рє Р±Р°Р·РѕРІРѕР№ РјРѕРґРµР»Рё">' + escapeHtml(row.mae_delta_display || '-') + '</td>'
                    + '<td data-label="РЎС‚Р°С‚СѓСЃ">' + escapeHtml(row.selection_label || '-') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderForecastTable(rows) {
        var container = byId('mlForecastTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">РџРѕСЃР»Рµ РѕР±СѓС‡РµРЅРёСЏ Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РїСЂРѕРіРЅРѕР· РїРѕ Р±СѓРґСѓС‰РёРј РґР°С‚Р°Рј.</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="forecast-table">'
            + '<thead><tr><th>Р”Р°С‚Р°</th><th>РћР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ</th><th>Р”РёР°РїР°Р·РѕРЅ</th><th>РРЅРґРµРєСЃ СЂРёСЃРєР°</th><th>РўРµРјРїРµСЂР°С‚СѓСЂР°</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Р”Р°С‚Р°">' + escapeHtml(row.date_display || '-') + '</td>'
                    + '<td data-label="РћР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ">' + escapeHtml(row.forecast_value_display || '0') + '</td>'
                    + '<td data-label="Р”РёР°РїР°Р·РѕРЅ">' + escapeHtml(row.range_display || 'вЂ”') + '</td>'
                    + '<td data-label="РРЅРґРµРєСЃ СЂРёСЃРєР°"><span class="ml-risk-pill ml-risk-' + escapeHtml(row.risk_level_tone || 'minimal') + '">' + escapeHtml(row.risk_index_display || '0 / 100') + '</span></td>'
                    + '<td data-label="РўРµРјРїРµСЂР°С‚СѓСЂР°">' + escapeHtml(row.temperature_display || 'вЂ”') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderFeatureCards(items) {
        var container = byId('mlFeatureCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІСЏС‚СЃСЏ РґР°РЅРЅС‹Рµ, РЅР° РєРѕС‚РѕСЂС‹С… СЂРµР°Р»СЊРЅРѕ РґРµСЂР¶РёС‚СЃСЏ РјРѕРґРµР»СЊ.</div>';
            return;
        }

        container.innerHTML = items.map(function (feature) {
            return ''
                + '<article class="forecast-feature-card status-' + escapeHtml(feature.status || 'missing') + '">'
                + '<div class="forecast-feature-head">'
                + '<strong>' + escapeHtml(feature.label || '-') + '</strong>'
                + '<span class="forecast-badge">' + escapeHtml(feature.status_label || '-') + '</span>'
                + '</div>'
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

    function showInitialSkeletons() {
        renderStatsSkeletons();
        renderCardSkeletons('mlQualityMetricCards', 4);
        renderOptionalMetricCards('mlQualityEventMetricsSection', 'mlQualityEventMetricCards', []);
        renderTableSkeleton('mlCountTableShell', 8, 4);
        charts.renderChartSkeleton('mlForecastChart', 'mlForecastChartFallback');
        renderTableSkeleton('mlForecastTableShell', 6, 4);
        charts.renderChartSkeleton('mlImportanceChart', 'mlImportanceChartFallback');
        renderImportanceNote('');
        renderFeatureSkeleton();
        renderCriticalNotes([]);
    }

    function applyMlModelData(data) {
        if (!data) {
            return;
        }

        currentMlData = data;

        var filters = data.filters || {};
        var summary = data.summary || {};
        var quality = data.quality_assessment || {};
        var chartData = data.charts || {};

        renderSidebarStatus(data);
        renderHero(data);
        renderSummaryCards(summary);

        setSelectOptions('mlTableFilter', filters.available_tables, filters.table_name, 'РќРµС‚ С‚Р°Р±Р»РёС†');
        setSelectOptions('mlHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Р’СЃРµ РіРѕРґС‹');
        setSelectOptions('mlCauseFilter', filters.available_causes, filters.cause, 'Р’СЃРµ РїСЂРёС‡РёРЅС‹');
        setSelectOptions('mlObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Р’СЃРµ РєР°С‚РµРіРѕСЂРёРё');
        setSelectOptions('mlForecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 РґРЅРµР№');
        setValue('mlTemperatureInput', filters.temperature || '');

        setText('mlQualityTitle', 'РќР°СЃРєРѕР»СЊРєРѕ РјРѕР¶РЅРѕ РґРѕРІРµСЂСЏС‚СЊ ML-РїСЂРѕРіРЅРѕР·Сѓ');
        setText('mlQualitySubtitle', quality.subtitle || 'Р§С‚Рѕ РїРѕРєР°Р·С‹РІР°РµС‚ Р±Р»РѕРє: РЅР°СЃРєРѕР»СЊРєРѕ РјРѕРґРµР»СЊ РїСЂРµРґСЃРєР°Р·С‹РІР°Р»Р° РёРјРµРЅРЅРѕ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РЅР° РїСЂРѕС€Р»РѕР№ РёСЃС‚РѕСЂРёРё Рё С‡РµРј РѕРЅР° Р»СѓС‡С€Рµ РїСЂРѕСЃС‚С‹С… РїРѕРґС…РѕРґРѕРІ.');
        renderMetricCards('mlQualityMetricCards', quality.metric_cards || [], 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІСЏС‚СЃСЏ РјРµС‚СЂРёРєРё РєР°С‡РµСЃС‚РІР° ML-РїСЂРѕРіРЅРѕР·Р°.');
        renderIntervalCoverage(quality.interval_card || null);
        renderOptionalMetricCards('mlQualityEventMetricsSection', 'mlQualityEventMetricCards', quality.event_metric_cards || [], '');
        setText('mlCountTableTitle', 'РЎСЂР°РІРЅРµРЅРёРµ РјРѕРґРµР»РµР№ РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ');
        renderCountTable(quality.count_table || {});
        setText('mlForecastTitle', 'РЎРєРѕР»СЊРєРѕ РїРѕР¶Р°СЂРѕРІ РѕР¶РёРґР°РµС‚СЃСЏ РїРѕ РґРЅСЏРј');
        charts.renderLineChart(chartData.forecast, 'mlForecastChart', 'mlForecastChartFallback');
        renderForecastTable(data.forecast_rows || []);

        setText('mlImportanceTitle', 'Р§С‚Рѕ СЃРёР»СЊРЅРµРµ РІСЃРµРіРѕ РІР»РёСЏРµС‚ РЅР° РїСЂРѕРіРЅРѕР·');
        charts.renderBarsChart(chartData.importance, 'mlImportanceChart', 'mlImportanceChartFallback');
        renderImportanceNote(chartData.importance && chartData.importance.note ? chartData.importance.note : '');
        renderFeatureCards(data.features || []);
        renderCriticalNotes(data.notes || []);
        updateMlScreenLinks({
            table_name: filters.table_name || 'all',
            cause: filters.cause || 'all',
            object_category: filters.object_category || 'all',
            temperature: filters.temperature || '',
            forecast_days: filters.forecast_days || '14',
            history_window: filters.history_window || 'all'
        });
    }

    function clearProgressTimers() {
        progressTimers.clear();
    }

    function updateProgressStep(activeIndex, options) {
        var settings = options || {};
        var activeStep = progressSteps[Math.max(0, Math.min(progressSteps.length - 1, activeIndex))];
        var leadText = settings.lead || activeStep.lead;
        var messageText = settings.message || activeStep.message;

        setStepProgress({
            activeIndex: activeIndex,
            isError: settings.isError,
            isFinished: settings.isFinished,
            lead: leadText,
            leadId: 'mlLoadingLead',
            message: messageText,
            messageId: 'mlLoadingMessage',
            stepSelector: '.ml-progress-step',
            stepsId: 'mlProgressSteps'
        });
    }

    function startProgressSequence() {
        runProgressSequence(progressTimers, updateProgressStep, [
            { activeIndex: 0 },
            { activeIndex: 1, delay: 350 },
            { activeIndex: 2, delay: 1100 },
            { activeIndex: 3, delay: 1800 }
        ]);
    }

    function setRefreshButtonState(isBusy) {
        var button = byId('mlRefreshButton');
        if (!button) {
            return;
        }
        button.disabled = !!isBusy;
        button.classList.toggle('is-loading', !!isBusy);
    }

    function setLoadingStateMode(mode) {
        var loadingState = byId('mlLoadingState');
        var skeleton = byId('mlLoadingSkeleton');
        if (!loadingState) {
            return;
        }
        loadingState.classList.remove('is-pending', 'is-ready');
        if (mode === 'ready') {
            loadingState.classList.add('is-ready');
        } else {
            loadingState.classList.add('is-pending');
        }
        if (skeleton) {
            skeleton.classList.toggle('is-hidden', mode === 'ready');
        }
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
            errorState.classList.add('is-hidden');
        }
        setText('mlErrorMessage', '');
        setLoadingStateMode('pending');
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
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        var activeIndex = 0;
        var currentJobState = api.getCurrentJobState ? api.getCurrentJobState() : null;

        if (asyncState) {
            asyncState.classList.remove('is-hidden');
        }
        if (loadingState) {
            loadingState.classList.remove('is-hidden');
        }
        if (errorState) {
            errorState.classList.remove('is-hidden');
        }
        if (currentJobState && currentJobState.status === 'running') {
            activeIndex = 1;
        }
        if (currentJobState && currentJobState.backtest_job
            && (currentJobState.backtest_job.status === 'running' || currentJobState.backtest_job.status === 'completed')) {
            activeIndex = 2;
        }
        setLoadingStateMode('ready');
        updateProgressStep(activeIndex, {
            isError: true,
            lead: 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РІРµСЂС€РёС‚СЊ ML-Р°РЅР°Р»РёР·',
            message: message || 'РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСѓСЃРє СЃ С‚РµРјРё Р¶Рµ С„РёР»СЊС‚СЂР°РјРё.'
        });
        setText('mlErrorMessage', message || 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ ML-РґР°РЅРЅС‹Рµ. РџРѕРїСЂРѕР±СѓР№С‚Рµ РµС‰Рµ СЂР°Р·.');
    }

    function hideError() {
        var asyncState = byId('mlAsyncState');
        var loadingState = byId('mlLoadingState');
        var errorState = byId('mlErrorState');
        if (errorState) {
            errorState.classList.add('is-hidden');
        }
        setText('mlErrorMessage', '');
        if (asyncState && loadingState && loadingState.classList.contains('is-hidden')) {
            asyncState.classList.add('is-hidden');
        }
    }

    function collectMlFiltersFromForm() {
        return {
            table_name: byId('mlTableFilter') ? byId('mlTableFilter').value : 'all',
            cause: byId('mlCauseFilter') ? byId('mlCauseFilter').value : 'all',
            object_category: byId('mlObjectCategoryFilter') ? byId('mlObjectCategoryFilter').value : 'all',
            temperature: byId('mlTemperatureInput') ? byId('mlTemperatureInput').value : '',
            forecast_days: byId('mlForecastDaysFilter') ? byId('mlForecastDaysFilter').value : '14',
            history_window: byId('mlHistoryWindowFilter') ? byId('mlHistoryWindowFilter').value : 'all'
        };
    }

    function buildMlNavigationHref(path, filters, options) {
        var safeFilters = filters || {};
        var settings = options || {};
        var params = new URLSearchParams();

        if (safeFilters.table_name && safeFilters.table_name !== 'all') {
            params.set('table_name', safeFilters.table_name);
        }
        if (!settings.onlyTable) {
            ['cause', 'object_category', 'temperature', 'forecast_days', 'history_window'].forEach(function (key) {
                var value = safeFilters[key];
                if (value != null && value !== '' && value !== 'all') {
                    params.set(key, value);
                }
            });
        }

        var query = params.toString();
        return path + (query ? '?' + query : '') + (settings.hash || '');
    }

    function updateMlScreenLinks(filters) {
        var safeFilters = filters || collectMlFiltersFromForm();
        setHref('mlPanelLink', buildMlNavigationHref('/', safeFilters, { onlyTable: true }));
        setHref('mlScenarioLink', buildMlNavigationHref('/forecasting', safeFilters));
        setHref('mlDecisionLink', buildMlNavigationHref('/forecasting', safeFilters, { hash: '#forecastDetails' }));
    }

    function updateAsyncStateForJob(jobPayload) {
        var safeJob = jobPayload || {};
        var backtestJob = safeJob.backtest_job || null;
        var activeIndex = 0;
        var lead = 'ML-Р·Р°РґР°С‡Р° РїРѕСЃС‚Р°РІР»РµРЅР° РІ РѕС‡РµСЂРµРґСЊ';
        var message = 'РћР¶РёРґР°РµРј Р·Р°РїСѓСЃРєР° С„РѕРЅРѕРІРѕРіРѕ СЂР°СЃС‡С‘С‚Р°.';
        var finished = false;

        if (safeJob.status === 'running') {
            activeIndex = 1;
            lead = 'РђРіСЂРµРіРёСЂСѓРµРј РёСЃС‚РѕСЂРёСЋ Рё РїСЂРёР·РЅР°РєРё';
            message = 'РЎРѕР±РёСЂР°РµРј SQL-Р°РіСЂРµРіР°С‚С‹, С„РёР»СЊС‚СЂС‹ Рё РґРЅРµРІРЅРѕР№ СЂСЏРґ РґР»СЏ ML-РїСЂРѕРіРЅРѕР·Р°.';
        }
        if (backtestJob && (backtestJob.status === 'running' || backtestJob.status === 'completed')) {
            activeIndex = 2;
            lead = backtestJob.status === 'completed' ? 'Р’Р°Р»РёРґР°С†РёСЏ Р·Р°РІРµСЂС€РµРЅР°' : 'Р’С‹РїРѕР»РЅСЏРµРј РѕР±СѓС‡РµРЅРёРµ Рё РІР°Р»РёРґР°С†РёСЋ';
            message = backtestJob.logs && backtestJob.logs.length
                ? backtestJob.logs[backtestJob.logs.length - 1]
                : 'РџСЂРѕРІРµСЂСЏРµРј РјРѕРґРµР»Рё РЅР° РёСЃС‚РѕСЂРёРё Рё РІС‹Р±РёСЂР°РµРј СЂР°Р±РѕС‡СѓСЋ РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ.';
        }
        if (safeJob.logs && safeJob.logs.length) {
            message = safeJob.logs[safeJob.logs.length - 1];
        }
        if (safeJob.status === 'completed') {
            activeIndex = 3;
            lead = 'ML-Р°РЅР°Р»РёР· Р·Р°РІРµСЂС€С‘РЅ';
            message = 'Р РµР·СѓР»СЊС‚Р°С‚ РіРѕС‚РѕРІ, РІРёР·СѓР°Р»РёР·Р°С†РёРё Рё С‚Р°Р±Р»РёС†С‹ СѓР¶Рµ РїРѕРґСЃС‚Р°РІР»РµРЅС‹ РІ РёРЅС‚РµСЂС„РµР№СЃ.';
            finished = true;
        }
        setLoadingStateMode(finished ? 'ready' : 'pending');
        updateProgressStep(activeIndex, {
            isFinished: finished,
            lead: lead,
            message: message
        });
    }

    function startMlModelJob(options) {
        var settings = options || {};
        api.startMlModelJob(settings, {
            onBusyChange: function (isBusy) {
                setRefreshButtonState(isBusy);
                renderSidebarStatus(currentMlData || global.__FIRE_ML_INITIAL__ || {});
            },
            onStart: function () {
                clearProgressTimers();
                showLoadingState();
                hideError();
                updateProgressStep(0, {
                    lead: 'ML-Р·Р°РґР°С‡Р° РїРѕСЃС‚Р°РІР»РµРЅР° РІ РѕС‡РµСЂРµРґСЊ',
                    message: 'РџРѕРґРіРѕС‚Р°РІР»РёРІР°РµРј С„РѕРЅРѕРІС‹Р№ Р·Р°РїСѓСЃРє Р°РЅР°Р»РёР·Р°.'
                });
                renderSidebarStatus(currentMlData || global.__FIRE_ML_INITIAL__ || {});

                if (settings.initialLoad) {
                    showInitialSkeletons();
                }
            },
            onJobState: function (payload) {
                updateAsyncStateForJob(payload);
            },
            onCompleted: function (result, payload) {
                applyMlModelData(result);
                updateAsyncStateForJob(payload || {});
                hideError();
                renderSidebarStatus(currentMlData || result || global.__FIRE_ML_INITIAL__ || {});
            },
            onError: function (message) {
                hideLoadingState();
                showError(message);
                renderSidebarStatus(currentMlData || global.__FIRE_ML_INITIAL__ || {});
            }
        });
    }

    function init() {
        var form = byId('mlModelForm');
        var initialData = global.__FIRE_ML_INITIAL__ || null;
        var syncScreenLinks = function () {
            updateMlScreenLinks(collectMlFiltersFromForm());
        };

        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                startMlModelJob();
            });
            Array.prototype.forEach.call(form.querySelectorAll('select, input'), function (field) {
                field.addEventListener('change', syncScreenLinks);
                if (field.tagName === 'INPUT') {
                    field.addEventListener('input', syncScreenLinks);
                }
            });
        }
        var retryButton = byId('mlRetryButton');
        if (retryButton) {
            retryButton.addEventListener('click', function () {
                startMlModelJob();
            });
        }

        syncScreenLinks();
        if (initialData && initialData.bootstrap_mode !== 'deferred') {
            applyMlModelData(initialData);
        } else {
            applyMlModelData(initialData || {});
            updateProgressStep(0, {
                lead: 'Р›С‘РіРєРёР№ shell СЃС‚СЂР°РЅРёС†С‹ СѓР¶Рµ РѕС‚РєСЂС‹С‚',
                message: 'Р—Р°РїСѓСЃРєР°РµРј ML-Р°РЅР°Р»РёР· РІ С„РѕРЅРµ Рё СЃР»РµРґРёРј Р·Р° СЃС‚Р°С‚СѓСЃРѕРј РїРѕ job_id.'
            });
            startMlModelJob({ initialLoad: true, useLocationSearch: true });
        }
    }

    global.MlModelRender = {
        applyMlModelData: applyMlModelData,
        init: init,
        startMlModelJob: startMlModelJob,
        updateMlScreenLinks: updateMlScreenLinks
    };
}(window));


