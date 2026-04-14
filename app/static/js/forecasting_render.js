(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};
    var applyToneClass = shared.applyToneClass;
    var byId = shared.byId;
    var escapeHtml = shared.escapeHtml;
    var setHref = shared.setHref;
    var setSelectOptions = shared.setSelectOptions;
    var setText = shared.setText;
    var setValue = shared.setValue;

    modules.createForecastingRender = function createForecastingRender(options) {
        var chartsApi = options && options.charts ? options.charts : {};
        var applyProgressBars = typeof chartsApi.applyProgressBars === 'function'
            ? chartsApi.applyProgressBars
            : function () {};
        var renderForecastCharts = typeof chartsApi.renderForecastCharts === 'function'
            ? chartsApi.renderForecastCharts
            : function () {};

    function renderInsights(items) {
        var container = byId('forecastInsights');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РЎРёРіРЅР°Р»С‹ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° РїСЂРѕРіРЅРѕР·Р°.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="insight-card tone-' + escapeHtml(item.tone || 'fire') + '">' +
                '<span class="insight-label">' + escapeHtml(item.label) + '</span>' +
                '<strong class="insight-value">' + escapeHtml(item.value) + '</strong>' +
                '<span class="insight-meta">' + escapeHtml(item.meta) + '</span>' +
            '</article>';
        }).join('');
        applyProgressBars(container);
    }

    function renderNotes(containerId, notes, emptyMessage) {
        var container = byId(containerId);
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

    function renderForecastTable(rows) {
        var container = byId('forecastTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІСЏС‚СЃСЏ Р±Р»РёР¶Р°Р№С€РёРµ РґР°С‚С‹ Рё РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР° РїРѕ СЃС†РµРЅР°СЂРёСЋ.</div>';
            return;
        }

        container.innerHTML = '<table class="forecast-table">' +
            '<thead><tr><th>Р”Р°С‚Р°</th><th>Р”РµРЅСЊ РЅРµРґРµР»Рё</th><th>Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР°</th><th>РљРѕРјРјРµРЅС‚Р°СЂРёР№</th></tr></thead>' +
            '<tbody>' + rows.map(function (row) {
                return '<tr>' +
                    '<td data-label="Р”Р°С‚Р°">' + escapeHtml(row.date_display) + '</td>' +
                    '<td data-label="Р”РµРЅСЊ РЅРµРґРµР»Рё">' + escapeHtml(row.weekday_label) + '</td>' +
                    '<td data-label="Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР°">' + escapeHtml(row.fire_probability_display || '0%') + '</td>' +
                    '<td data-label="РљРѕРјРјРµРЅС‚Р°СЂРёР№"><span class="forecast-scenario-pill tone-' + escapeHtml(row.scenario_tone || 'sky') + '">' + escapeHtml(row.scenario_label || 'РћРєРѕР»Рѕ РѕР±С‹С‡РЅРѕРіРѕ') + '</span><div class="forecast-cell-note">' + escapeHtml(row.scenario_hint || '') + '</div></td>' +
                '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderRiskSummary(items) {
        var container = byId('forecastRiskCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РљР°СЂС‚РѕС‡РєРё Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="insight-card tone-' + escapeHtml(item.tone || 'sky') + '">' +
                '<span class="insight-label">' + escapeHtml(item.label) + '</span>' +
                '<strong class="insight-value">' + escapeHtml(item.value) + '</strong>' +
                '<span class="insight-meta">' + escapeHtml(item.meta) + '</span>' +
            '</article>';
        }).join('');
    }

    function findComponent(item, key) {
        var items = Array.isArray(item && item.component_scores) ? item.component_scores : [];
        for (var index = 0; index < items.length; index += 1) {
            if (items[index] && items[index].key === key) {
                return items[index];
            }
        }
        return null;
    }

    function renderRiskTerritories(items) {
        var container = byId('forecastRiskTerritories');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ СЂР°РЅР¶РёСЂРѕРІР°РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёР№ РґР»СЏ РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            var components = Array.isArray(item.component_scores) ? item.component_scores : [];
            var recommendations = Array.isArray(item.recommendations) ? item.recommendations : [];
            var rankingTone = normalizeTone(item.ranking_confidence_tone || 'fire');
            var whyText = item.ranking_reason || item.drivers_display || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ РїСЂРёРѕСЂРёС‚РµС‚Р°.';
            var reliabilityText = item.ranking_confidence_note || 'РћС†РµРЅРєР° РЅР°РґС‘Р¶РЅРѕСЃС‚Рё РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.';
            var metricOrder = [
                { key: 'fire_frequency', fallback: 'Р§Р°СЃС‚РѕС‚Р° РїРѕР¶Р°СЂРѕРІ' },
                { key: 'consequence_severity', fallback: 'РўСЏР¶РµСЃС‚СЊ РїРѕСЃР»РµРґСЃС‚РІРёР№' },
                { key: 'long_arrival_risk', fallback: 'Р”РѕР»РіРѕРµ РїСЂРёР±С‹С‚РёРµ' },
                { key: 'water_supply_deficit', fallback: 'Р”РµС„РёС†РёС‚ РІРѕРґС‹' }
            ];

            var metricsHtml = metricOrder.map(function (descriptor) {
                var component = findComponent(item, descriptor.key);
                return '<div><span>' + escapeHtml(component ? component.label : descriptor.fallback) + '</span><strong>' + escapeHtml(component ? component.score_display : '0 / 100') + '</strong></div>';
            }).join('');

            var componentsHtml = components.map(function (component) {
                return '<article class="risk-component-card tone-' + escapeHtml(component.tone || 'low') + '">' +
                    '<div class="risk-component-head"><strong>' + escapeHtml(component.label || 'РљРѕРјРїРѕРЅРµРЅС‚') + '</strong><span>' + escapeHtml(component.score_display || '0 / 100') + '</span></div>' +
                    '<div class="risk-component-bar"><span data-bar-width="' + escapeHtml(component.bar_width || '12%') + '"></span></div>' +
                    '<div class="risk-component-meta">' + escapeHtml(component.summary || '') + '</div>' +
                    '<p>' + escapeHtml(component.rationale || '') + '</p>' +
                '</article>';
            }).join('');

            var recommendationsHtml = recommendations.length ? recommendations.map(function (recommendation) {
                return '<article class="risk-recommendation-item">' +
                    '<strong>' + escapeHtml(recommendation.label || 'Р РµРєРѕРјРµРЅРґР°С†РёСЏ') + '</strong>' +
                    '<span>' + escapeHtml(recommendation.detail || '') + '</span>' +
                '</article>';
            }).join('') : '<div class="mini-empty">Р РµРєРѕРјРµРЅРґР°С†РёРё РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.</div>';

            return '<article class="risk-territory-card tone-' + escapeHtml(item.risk_tone || 'low') + '">' +
                '<div class="risk-territory-head">' +
                    '<div>' +
                        '<strong>' + escapeHtml(item.label) + '</strong>' +
                        '<div class="risk-territory-tags">' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.risk_tone || 'low') + '">' + escapeHtml(item.risk_class_label || 'РќРёР·РєРёР№ СЂРёСЃРє') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.priority_tone || 'sky') + '">' + escapeHtml(item.priority_label || 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-sky">' + escapeHtml(item.weight_mode_label || 'Р­РєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР°') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(rankingTone) + '">' + escapeHtml(item.ranking_confidence_label || 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ') + '</span>' +
                        '</div>' +
                    '</div>' +
                    '<div class="risk-territory-score">' + escapeHtml(item.risk_display || '0 / 100') + '</div>' +
                '</div>' +
                '<div class="risk-score-bar"><span data-bar-width="' + escapeHtml(item.bar_width || '10%') + '"></span></div>' +
                '<div class="risk-territory-callout">' +
                    '<span>Р§С‚Рѕ РїСЂРѕРІРµСЂРёС‚СЊ РїРµСЂРІС‹Рј</span>' +
                    '<strong>' + escapeHtml(item.action_label || 'РћСЃС‚Р°РІРёС‚СЊ С‚РµСЂСЂРёС‚РѕСЂРёСЋ РІ РїР»Р°РЅРѕРІРѕРј РЅР°Р±Р»СЋРґРµРЅРёРё') + '</strong>' +
                    '<p>' + escapeHtml(item.action_hint || '') + '</p>' +
                '</div>' +
                '<div class="risk-metrics-grid">' + metricsHtml + '</div>' +
                '<div class="risk-components-grid">' + componentsHtml + '</div>' +
                '<p class="risk-formula"><strong>РљР°Рє СЃР»РѕР¶РёР»СЃСЏ РёС‚РѕРіРѕРІС‹Р№ Р±Р°Р»Р»:</strong> ' + escapeHtml(item.risk_formula_display || '') + '</p>' +
                '<div class="risk-recommendation-list">' + recommendationsHtml + '</div>' +
                '<div class="risk-territory-meta">' +
                    '<span>РљРѕРЅС‚РµРєСЃС‚: <strong>' + escapeHtml(item.settlement_context_label || 'РќРµ СѓРєР°Р·Р°РЅ') + '</strong></span>' +
                    '<span>РџРѕСЃР»РµРґРЅРёР№ РїРѕР¶Р°СЂ: <strong>' + escapeHtml(item.last_fire_display || '-') + '</strong></span>' +
                    '<span>Travel-time: <strong>' + escapeHtml(item.travel_time_display || 'РЅ/Рґ') + '</strong></span>' +
                    '<span>РЎСЂРµРґРЅРµРµ РїСЂРёР±С‹С‚РёРµ: <strong>' + escapeHtml(item.response_time_display || 'РќРµС‚ РґР°РЅРЅС‹С…') + '</strong></span>' +
                    '<span>РЈРґР°Р»С‘РЅРЅРѕСЃС‚СЊ РѕС‚ РџР§: <strong>' + escapeHtml(item.distance_display || 'РќРµС‚ РґР°РЅРЅС‹С…') + '</strong></span>' +
                    '<span>РџРѕРєСЂС‹С‚РёРµ РџР§: <strong>' + escapeHtml(item.fire_station_coverage_display || 'РЅ/Рґ') + ' (' + escapeHtml(item.fire_station_coverage_label || 'РЅРµС‚ РґР°РЅРЅС‹С…') + ')</strong></span>' +
                    '<span>РЎРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР°: <strong>' + escapeHtml(item.service_zone_label || 'РЅРµ РѕРїСЂРµРґРµР»РµРЅР°') + '</strong></span>' +
                    '<span>Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ РїСЂРёРѕСЂРёС‚РµС‚: <strong>' + escapeHtml(item.logistics_priority_display || '0 / 100') + '</strong></span>' +
                    '<span>Р’РѕРґР°: <strong>' + escapeHtml(item.water_supply_display || 'РќРµС‚ РґР°РЅРЅС‹С…') + '</strong></span>' +
                    '<span>РћР±СЉРµРєС‚С‹: <strong>' + escapeHtml(item.dominant_object_category || 'РќРµ СѓРєР°Р·Р°РЅРѕ') + '</strong></span>' +
                '</div>' +
                '<p class="risk-drivers"><strong>РџРѕС‡РµРјСѓ РёРјРµРЅРЅРѕ СЌС‚Р° С‚РµСЂСЂРёС‚РѕСЂРёСЏ:</strong> ' + escapeHtml(whyText) + '</p>' +
                '<p class="risk-drivers"><strong>РџРѕС‡РµРјСѓ СѓСЂРѕРІРµРЅСЊ РґРѕРІРµСЂРёСЏ С‚Р°РєРѕР№:</strong> ' + escapeHtml(reliabilityText) + '</p>' +
            '</article>';
        }).join('');
    }
    function renderFeatureCards(items) {
        var container = byId('forecastFeatureCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">РЎРїРёСЃРѕРє РїСЂРёР·РЅР°РєРѕРІ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="forecast-feature-card status-' + escapeHtml(item.status || 'missing') + '">' +
                '<div class="forecast-feature-head">' +
                    '<strong>' + escapeHtml(item.label) + '</strong>' +
                    '<span class="forecast-badge">' + escapeHtml(item.status_label || 'РќРµ РЅР°Р№РґРµРЅР°') + '</span>' +
                '</div>' +
                '<p>' + escapeHtml(item.description || '') + '</p>' +
                '<div class="forecast-feature-source">' + escapeHtml(item.source || 'РќРµ РЅР°Р№РґРµРЅР°') + '</div>' +
            '</article>';
        }).join('');
    }

    var currentForecastData = window.__FIRE_FORECAST_INITIAL__ || null;

    function setForecastAsyncVisibility(visible) {
        var asyncNode = byId('forecastAsyncState');
        if (!asyncNode) {
            return;
        }
        asyncNode.classList.toggle('is-hidden', !visible);
    }

    function setForecastStageVisibility(stageName, visible) {
        Array.prototype.forEach.call(
            document.querySelectorAll('[data-forecast-stage~="' + stageName + '"]'),
            function (node) {
                node.hidden = !visible;
            }
        );
    }

    function syncForecastStageVisibility(data) {
        var safeData = data || {};
        setForecastStageVisibility(
            'metadata',
            Boolean(safeData.metadata_ready || (!safeData.metadata_pending && !safeData.deferred))
        );
        setForecastStageVisibility(
            'base',
            Boolean(safeData.base_forecast_ready || (!safeData.base_forecast_pending && !safeData.loading && !safeData.deferred))
        );
        setForecastStageVisibility(
            'decision',
            Boolean(
                safeData.decision_support_ready ||
                (!safeData.decision_support_pending && !safeData.deferred && !safeData.base_forecast_pending && !safeData.loading)
            )
        );
    }

    function hideForecastError() {
        var errorNode = byId('forecastErrorState');
        var runtimeNode = byId('forecastJobRuntime');
        if (!errorNode) {
            return;
        }
        errorNode.classList.add('is-hidden');
        setText('forecastErrorMessage', '');
        if (!runtimeNode || runtimeNode.classList.contains('is-hidden')) {
            setForecastAsyncVisibility(false);
        }
    }

    function showForecastError(message) {
        var errorNode = byId('forecastErrorState');
        setForecastAsyncVisibility(true);
        setText('forecastErrorMessage', message || 'РќРµ СѓРґР°Р»РѕСЃСЊ РїРµСЂРµСЃС‡РёС‚Р°С‚СЊ РїСЂРѕРіРЅРѕР·. РџРѕРїСЂРѕР±СѓР№С‚Рµ РµС‰Рµ СЂР°Р·.');
        if (errorNode) {
            errorNode.classList.remove('is-hidden');
        }
    }

    function syncSidebarBadge(data) {
        var node = document.querySelector('.sidebar-status .status-badge');
        if (!node) {
            return;
        }

        if (data && data.bootstrap_mode === 'deferred') {
            node.textContent = 'РџРѕРґРіРѕС‚Р°РІР»РёРІР°РµРј РїСЂРѕРіРЅРѕР·';
            node.classList.add('status-badge-live');
            return;
        }
        if (data && data.has_data) {
            node.textContent = 'РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· СЃРѕР±СЂР°РЅ';
            node.classList.add('status-badge-live');
            return;
        }
        node.textContent = 'РќСѓР¶РЅРѕ СѓС‚РѕС‡РЅРёС‚СЊ С„РёР»СЊС‚СЂС‹';
        node.classList.remove('status-badge-live');
    }

    function buildSummaryLine(summary, data) {
        var safeSummary = summary || {};
        if (data && data.metadata_pending && data.metadata_status_message) {
            return data.metadata_status_message;
        }
        if (data && data.loading && data.loading_status_message) {
            return data.loading_status_message;
        }
        return 'РЎРµР№С‡Р°СЃ РїРѕРєР°Р·Р°РЅРѕ: ' + (safeSummary.slice_label || 'Р’СЃРµ РїРѕР¶Р°СЂС‹') +
            ' | РўРёРїРёС‡РЅС‹Р№ РґРµРЅСЊ: ' + (safeSummary.average_probability_display || '0%') +
            ' | РџРёРє: ' + (safeSummary.peak_forecast_probability_display || '0%') + ' (' + (safeSummary.peak_forecast_day_display || '-') + ')' +
            ' | Рљ РїРѕСЃР»РµРґРЅРёРј 4 РЅРµРґРµР»СЏРј: ' + (safeSummary.forecast_vs_recent_display || '0%');
    }

    function clearForecastJobRuntime(runtimeNode, titleNode, metaNode, logsNode) {
        runtimeNode.classList.add('is-hidden');
        runtimeNode.classList.remove('is-ready');
        titleNode.textContent = 'Р“РѕС‚РѕРІРёРј Р±Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№';
        metaNode.textContent = '';
        logsNode.textContent = '';
    }

    function shouldShowForecastJobRuntime(jobPayload) {
        return Boolean(
            jobPayload &&
            jobPayload.job_id &&
            jobPayload.status !== 'completed' &&
            jobPayload.status !== 'failed' &&
            jobPayload.status !== 'missing'
        );
    }

    function getForecastJobRuntimeTitle(jobPayload) {
        var safeJob = jobPayload || {};
        var meta = safeJob.meta || {};

        if (safeJob.reused) {
            return 'РџРѕРґРєР»СЋС‡Р°РµРј СѓР¶Рµ Р·Р°РїСѓС‰РµРЅРЅС‹Р№ СЂР°СЃС‡С‘С‚';
        }
        if (meta.stage_label) {
            return String(meta.stage_label);
        }
        if (safeJob.status === 'pending') {
            return 'Р“РѕС‚РѕРІРёРј Р±Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№';
        }
        return 'РЎРѕР±РёСЂР°РµРј Р±Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№';
    }

    function getForecastJobRuntimeMeta(jobPayload) {
        var safeJob = jobPayload || {};
        var meta = safeJob.meta || {};
        var metaParts = [];

        if (meta.stage_message) {
            metaParts.push(String(meta.stage_message));
        }
        if (safeJob.reused) {
            metaParts.push('РёСЃРїРѕР»СЊР·СѓРµРј СѓР¶Рµ Р·Р°РїСѓС‰РµРЅРЅС‹Р№ СЂР°СЃС‡С‘С‚');
        }
        return metaParts.join(' | ');
    }

    function renderForecastJobRuntime(jobPayload) {
        var runtimeNode = byId('forecastJobRuntime');
        var titleNode = byId('forecastJobRuntimeTitle');
        var metaNode = byId('forecastJobMeta');
        var logsNode = byId('forecastJobLogOutput');
        var safeJob = jobPayload || {};
        var logs = Array.isArray(safeJob.logs) ? safeJob.logs : [];
        var errorNode = byId('forecastErrorState');

        if (!runtimeNode || !titleNode || !metaNode || !logsNode) {
            return;
        }
        if (!shouldShowForecastJobRuntime(safeJob)) {
            clearForecastJobRuntime(runtimeNode, titleNode, metaNode, logsNode);
            if (!errorNode || errorNode.classList.contains('is-hidden')) {
                setForecastAsyncVisibility(false);
            }
            return;
        }

        setForecastAsyncVisibility(true);
        runtimeNode.classList.remove('is-hidden');
        runtimeNode.classList.remove('is-ready');
        titleNode.textContent = getForecastJobRuntimeTitle(safeJob);
        metaNode.textContent = getForecastJobRuntimeMeta(safeJob);
        logsNode.textContent = logs.length ? logs.join('\n') : 'РџРѕРєР°Р¶РµРј РїСЂРѕРіСЂРµСЃСЃ, РєР°Рє С‚РѕР»СЊРєРѕ СЂР°СЃС‡С‘С‚ РїРµСЂРµР№РґС‘С‚ Рє СЃР»РµРґСѓСЋС‰РµРјСѓ СЌС‚Р°РїСѓ.';
        return;
        /* Legacy technical runtime rendering removed.
        if (meta.cache_hit) {
            metaParts.push('РєСЌС€');
        }
        if (safeJob.reused) {
            metaParts.push('РїРµСЂРµРёСЃРїРѕР»СЊР·РѕРІР°РЅ');
        }
        metaNode.textContent = metaParts.join(' | ');
        logsNode.textContent = logs.length ? logs.join('\n') : 'Р›РѕРіРё РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ Р·Р°РїСѓСЃРєР° С„РѕРЅРѕРІРѕР№ Р·Р°РґР°С‡Рё.';
        */
    }

    function updateDecisionSupportJobState(jobPayload) {
        renderForecastJobRuntime(jobPayload || {});
    }

    function normalizeTone(tone) {
        if (tone === 'high') {
            return 'fire';
        }
        if (tone === 'medium') {
            return 'sand';
        }
        if (tone === 'low') {
            return 'sky';
        }
        return tone || 'sky';
    }

    function renderWeightProfile(profile) {
        var safeProfile = profile || {};
        var cardsContainer = byId('forecastWeightProfileCards');
        var notes = [];
        var components = Array.isArray(safeProfile.components) ? safeProfile.components : [];

        setText('forecastWeightProfileDescription', safeProfile.description || 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РїРѕРЅСЏС‚РЅРѕРµ РѕР±СЉСЏСЃРЅРµРЅРёРµ, РєР°РєРёРµ С„Р°РєС‚РѕСЂС‹ СЃРёР»СЊРЅРµРµ РІСЃРµРіРѕ РґРІРёРіР°СЋС‚ С‚РµСЂСЂРёС‚РѕСЂРёСЋ РІРІРµСЂС… РёР»Рё РІРЅРёР·.');
        setText('forecastWeightModeBadge', safeProfile.status_label || 'РђРєС‚РёРІРЅС‹Р№ РїСЂРѕС„РёР»СЊ');
        applyToneClass(byId('forecastWeightModeBadge'), safeProfile.status_tone || 'forest');

        if (cardsContainer) {
            if (!components.length) {
                cardsContainer.innerHTML = '<div class="mini-empty">РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ СЃРїРёСЃРѕРє С„Р°РєС‚РѕСЂРѕРІ, РєРѕС‚РѕСЂС‹Рµ Р±РѕР»СЊС€Рµ РІСЃРµРіРѕ РІР»РёСЏСЋС‚ РЅР° РёС‚РѕРіРѕРІС‹Р№ Р±Р°Р»Р».</div>';
            } else {
                cardsContainer.innerHTML = components.map(function (item) {
                    return '<article class="risk-weight-card">' +
                        '<div class="risk-weight-head"><strong>' + escapeHtml(item.label || 'РљРѕРјРїРѕРЅРµРЅС‚') + '</strong><span>' + escapeHtml(item.current_weight_display || item.weight_display || '0%') + '</span></div>' +
                        '<p>' + escapeHtml(item.description || '') + '</p>' +
                        '<div class="risk-weight-meta">' +
                            '<span>Р­РєСЃРїРµСЂС‚: <strong>' + escapeHtml(item.expert_weight_display || item.weight_display || '0%') + '</strong></span>' +
                            '<span>РўРµРєСѓС‰РёР№: <strong>' + escapeHtml(item.current_weight_display || item.weight_display || '0%') + '</strong></span>' +
                            '<span>РљР°Р»РёР±СЂРѕРІРєР°: <strong>' + escapeHtml(item.calibration_shift_display || '0 Рї.Рї.') + '</strong></span>' +
                            '<span>РЎРµР»СЊСЃРєРёР№ РєРѕРЅС‚СѓСЂ: <strong>' + escapeHtml(item.rural_weight_display || item.weight_display || '0%') + '</strong></span>' +
                        '</div>' +
                    '</article>';
                }).join('');
            }
        }

        [].concat(safeProfile.notes || [], safeProfile.calibration_notes || []).forEach(function (note) {
            var text = String(note || '').trim();
            if (text && notes.indexOf(text) === -1) {
                notes.push(text);
            }
        });
        renderNotes('forecastWeightProfileNotes', notes, 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЏСЃРЅРµРЅРёСЏ, РїРѕС‡РµРјСѓ РїСЂРѕС„РёР»СЊ РІРµСЃРѕРІ РІС‹РіР»СЏРґРёС‚ РёРјРµРЅРЅРѕ С‚Р°Рє.');
    }

    function renderCommandCards(brief) {
        var container = byId('forecastCommandCards');
        var cards = brief && Array.isArray(brief.cards) ? brief.cards : [];

        if (!container) {
            return;
        }

        if (!cards.length) {
            container.innerHTML = '<div class="mini-empty">РљРѕСЂРѕС‚РєРёР№ РІС‹РІРѕРґ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.</div>';
            return;
        }

        container.innerHTML = cards.map(function (item) {
            return '<article class="executive-brief-card tone-' + escapeHtml(item.tone || 'sky') + '">' +
                '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>' +
                '<strong class="stat-value executive-brief-value">' + escapeHtml(item.value || '-') + '</strong>' +
                '<span class="stat-foot">' + escapeHtml(item.meta || '') + '</span>' +
            '</article>';
        }).join('');
    }

    function renderCommandNotes(brief) {
        var notes = brief && Array.isArray(brief.notes) ? brief.notes.slice(0, 3) : [];
        renderNotes('forecastCommandNotes', notes, 'РћРіСЂР°РЅРёС‡РµРЅРёСЏ Рё РїСЂРёРјРµС‡Р°РЅРёСЏ РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.');
    }

    function buildForecastBriefHref(filters) {
        var params = new URLSearchParams();
        var safeFilters = filters || {};

        [
            'table_name',
            'district',
            'cause',
            'object_category',
            'temperature',
            'forecast_days',
            'history_window'
        ].forEach(function (key) {
            var value = safeFilters[key];
            if (value != null && value !== '') {
                params.set(key, value);
            }
        });

        var query = params.toString();
        return '/brief/forecasting.txt' + (query ? '?' + query : '');
    }

    function updateForecastBriefExport(filters) {
        var href = buildForecastBriefHref(filters || {});
        Array.prototype.forEach.call(
            document.querySelectorAll('#decisionSupportPanel .executive-brief-download, #decisionSupportPanel .executive-brief-summary-action'),
            function (link) {
                link.setAttribute('href', href);
            }
        );
    }

    function buildForecastNavigationHref(path, filters, options) {
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

    function updateForecastScreenLinks(filters) {
        var safeFilters = filters || collectForecastFiltersFromForm();
        setHref('forecastPanelLink', buildForecastNavigationHref('/', safeFilters, { onlyTable: true }));
        setHref('forecastMlLink', buildForecastNavigationHref('/ml-model', safeFilters));
    }

    function collectForecastFiltersFromForm() {
        return {
            table_name: byId('forecastTableFilter') ? byId('forecastTableFilter').value : '',
            district: byId('forecastDistrictFilter') ? byId('forecastDistrictFilter').value : 'all',
            cause: byId('forecastCauseFilter') ? byId('forecastCauseFilter').value : 'all',
            object_category: byId('forecastObjectCategoryFilter') ? byId('forecastObjectCategoryFilter').value : 'all',
            temperature: byId('forecastTemperatureInput') ? byId('forecastTemperatureInput').value : '',
            forecast_days: byId('forecastDaysFilter') ? byId('forecastDaysFilter').value : '',
            history_window: byId('forecastHistoryWindowFilter') ? byId('forecastHistoryWindowFilter').value : ''
        };
    }

    function buildAnalyticalBrief(data) {
        var summary = data.summary || {};
        var quality = data.quality_assessment || {};
        var risk = data.risk_prediction || {};
        var passport = risk.quality_passport || {};
        var territories = Array.isArray(risk.territories) ? risk.territories : [];
        var weightProfile = risk.weight_profile || {};
        var notes = [];
        var seenNotes = {};

        [].concat(passport.reliability_notes || [], weightProfile.notes || [], risk.notes || [], data.notes || []).forEach(function (note) {
            var text = String(note || '').trim();
            if (text && !seenNotes[text]) {
                seenNotes[text] = true;
                notes.push(text);
            }
        });

        var lines = [
            'РљСЂР°С‚РєР°СЏ СЃРїСЂР°РІРєР°: СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· Рё РїРѕРґРґРµСЂР¶РєР° СЂРµС€РµРЅРёР№',
            'РЎС„РѕСЂРјРёСЂРѕРІР°РЅРѕ: ' + (data.generated_at || '-'),
            '',
            'РЎСЂРµР· Р°РЅР°Р»РёР·Р°',
            'РўР°Р±Р»РёС†Р°: ' + (summary.selected_table_label || 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹'),
            'РСЃС‚РѕСЂРёСЏ: ' + (summary.history_window_label || 'Р’СЃРµ РіРѕРґС‹'),
            'РЎСЂРµР·: ' + (summary.slice_label || 'Р’СЃРµ РїРѕР¶Р°СЂС‹'),
            'Р“РѕСЂРёР·РѕРЅС‚ РїСЂРѕРіРЅРѕР·Р°: ' + (summary.forecast_days_display || '0') + ' РґРЅРµР№',
            '',
            'РќР°СЃРєРѕР»СЊРєРѕ РїСЂРѕРіРЅРѕР· РїРѕ РґРЅСЏРј РїРѕРїР°РґР°РµС‚ РІ РёСЃС‚РѕСЂРёСЋ',
            'РЎС‚Р°С‚СѓСЃ: ' + (quality.title || 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё'),
            'РљРѕРјРјРµРЅС‚Р°СЂРёР№: ' + (quality.subtitle || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕС†РµРЅРєРё РєР°С‡РµСЃС‚РІР°.'),
            '',
            'РџРѕС‡РµРјСѓ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РїРѕРґРЅСЏР»Р°СЃСЊ РІРІРµСЂС… РІ РїСЂРёРѕСЂРёС‚РµС‚Рµ',
            'Р РµР¶РёРј: ' + (weightProfile.mode_label || 'Р­РєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР°'),
            'РћРїРёСЃР°РЅРёРµ: ' + (weightProfile.description || 'РќРµС‚ РѕРїРёСЃР°РЅРёСЏ.'),
        ];

        (quality.metric_cards || []).forEach(function (item) {
            lines.push('- ' + (item.label || 'РњРµС‚СЂРёРєР°') + ': ' + (item.value || '-') + ' | ' + (item.meta || ''));
        });
        (quality.dissertation_points || []).forEach(function (item) {
            lines.push('- ' + item);
        });

        if (Array.isArray(weightProfile.components) && weightProfile.components.length) {
            weightProfile.components.forEach(function (item) {
                lines.push('- ' + (item.label || 'РљРѕРјРїРѕРЅРµРЅС‚') + ': СЌРєСЃРїРµСЂС‚ ' + (item.expert_weight_display || item.weight_display || '0%') + ', С‚РµРєСѓС‰РёР№ ' + (item.current_weight_display || item.weight_display || '0%') + ', РєР°Р»РёР±СЂРѕРІРєР° ' + (item.calibration_shift_display || '0 Рї.Рї.') + ', СЃРµР»СЊСЃРєРёР№ РєРѕРЅС‚СѓСЂ ' + (item.rural_weight_display || item.weight_display || '0%'));
            });
        }

        lines.push('РќР°РґС‘Р¶РЅРѕСЃС‚СЊ РІС‹РІРѕРґР° РїРѕ С‚РµСЂСЂРёС‚РѕСЂРёРё-Р»РёРґРµСЂСѓ: ' + ((risk.top_territory_confidence_label || (territories[0] && territories[0].ranking_confidence_label) || 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ')) + ' (' + ((risk.top_territory_confidence_score_display || (territories[0] && territories[0].ranking_confidence_display) || '0 / 100')) + ')');
        lines.push('РџРѕСЏСЃРЅРµРЅРёРµ: ' + ((risk.top_territory_confidence_note || (territories[0] && territories[0].ranking_confidence_note) || 'РќРµС‚ РїРѕСЏСЃРЅРµРЅРёСЏ РїРѕ РЅР°РґС‘Р¶РЅРѕСЃС‚Рё РІС‹РІРѕРґР°.')));

        lines.push('', 'РџСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ С‚РµСЂСЂРёС‚РѕСЂРёРё');
        if (territories.length) {
            territories.slice(0, 5).forEach(function (item, index) {
                lines.push((index + 1) + '. ' + (item.label || 'РўРµСЂСЂРёС‚РѕСЂРёСЏ'));
                lines.push('   Р РёСЃРє: ' + (item.risk_display || '0 / 100') + ' | РљР»Р°СЃСЃ: ' + (item.risk_class_label || '-') + ' | РџСЂРёРѕСЂРёС‚РµС‚: ' + (item.priority_label || '-'));
                lines.push('   Р¤РѕСЂРјСѓР»Р°: ' + (item.risk_formula_display || 'РќРµС‚ С„РѕСЂРјСѓР»С‹.'));
                lines.push('   Р›РѕРіРёСЃС‚РёРєР°: travel-time ' + (item.travel_time_display || 'РЅ/Рґ') + ', РїРѕРєСЂС‹С‚РёРµ РџР§ ' + (item.fire_station_coverage_display || 'РЅ/Рґ') + ', СЃРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР° ' + (item.service_zone_label || 'РЅРµ РѕРїСЂРµРґРµР»РµРЅР°') + ', Р»РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ РїСЂРёРѕСЂРёС‚РµС‚ ' + (item.logistics_priority_display || '0 / 100') + '.');
                (item.component_scores || []).forEach(function (component) {
                    lines.push('   - ' + (component.label || 'РљРѕРјРїРѕРЅРµРЅС‚') + ': ' + (component.score_display || '0 / 100') + ', РІРµСЃ ' + (component.weight_display || '0%') + ', РІРєР»Р°Рґ ' + (component.contribution_display || '0 Р±Р°Р»Р»Р°'));
                });
                lines.push('   РџРѕС‡РµРјСѓ: ' + (item.ranking_reason || item.drivers_display || 'РќРµС‚ РїРѕСЏСЃРЅРµРЅРёСЏ.'));
                lines.push('   РќР°РґС‘Р¶РЅРѕСЃС‚СЊ: ' + ((item.ranking_confidence_label || 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ')) + ' (' + (item.ranking_confidence_display || '0 / 100') + ')');
                lines.push('   РџРѕСЏСЃРЅРµРЅРёРµ: ' + (item.ranking_confidence_note || 'РќРµС‚ РїРѕСЏСЃРЅРµРЅРёСЏ РїРѕ РЅР°РґС‘Р¶РЅРѕСЃС‚Рё.'));
                lines.push('   Р§С‚Рѕ СЃРґРµР»Р°С‚СЊ РїРµСЂРІС‹Рј: ' + (item.action_label || 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ') + '. ' + (item.action_hint || ''));
            });
        } else {
            lines.push('РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ С‚РµСЂСЂРёС‚РѕСЂРёР№.');
        }

        lines.push('', 'РћРіСЂР°РЅРёС‡РµРЅРёСЏ Рё Р·Р°РјРµС‡Р°РЅРёСЏ');
        if (notes.length) {
            notes.slice(0, 10).forEach(function (note, index) {
                lines.push((index + 1) + '. ' + note);
            });
        } else {
            lines.push('1. РЎСѓС‰РµСЃС‚РІРµРЅРЅС‹С… РѕРіСЂР°РЅРёС‡РµРЅРёР№ РІ С‚РµРєСѓС‰РµРј СЃСЂРµР·Рµ РЅРµ Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅРѕ.');
        }

        return lines.join('\r\n');
    }
    function downloadAnalyticalBrief() {
        var data = currentForecastData || window.__FIRE_FORECAST_INITIAL__;
        if (!data) {
            return;
        }

        var text = buildAnalyticalBrief(data);
        var stampSource = String(data.generated_at || '').replace(/\D/g, '').slice(0, 12) || 'report';
        var blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
        var url = window.URL.createObjectURL(blob);
        var link = document.createElement('a');
        link.href = url;
        link.download = 'fire-risk-brief-' + stampSource + '.txt';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
    }
    function applyForecastData(data) {
        if (!data) {
            return;
        }

        var filters = data.filters || {};
        var summary = data.summary || {};
        var charts = data.charts || {};
        var risk = data.risk_prediction || {};
        var executiveBrief = data.executive_brief || {};
        var passport = risk.quality_passport || {};
        var territories = Array.isArray(risk.territories) ? risk.territories : [];
        var leadTerritory = territories[0] || {};

        currentForecastData = data;

        setSelectOptions('forecastTableFilter', filters.available_tables, filters.table_name, 'РќРµС‚ С‚Р°Р±Р»РёС†');
        setSelectOptions('forecastHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Р’СЃРµ РіРѕРґС‹');
        setSelectOptions('forecastDistrictFilter', filters.available_districts, filters.district, 'Р’СЃРµ СЂР°Р№РѕРЅС‹');
        setSelectOptions('forecastCauseFilter', filters.available_causes, filters.cause, 'Р’СЃРµ РїСЂРёС‡РёРЅС‹');
        setSelectOptions('forecastObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Р’СЃРµ РєР°С‚РµРіРѕСЂРёРё');
        setSelectOptions('forecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 РґРЅРµР№');
        setValue('forecastTemperatureInput', filters.temperature || '');

        setText('forecastModelDescription', data.model_description || '');
        setText('forecastLeadSummary', summary.hero_summary || executiveBrief.lead || risk.top_territory_explanation || 'РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ РІС‹РІРѕРґ РїРѕ РґР°С‚Р°Рј, РіРґРµ СЃС†РµРЅР°СЂРёР№ РІС‹РіР»СЏРґРёС‚ РЅР°РїСЂСЏР¶С‘РЅРЅРµРµ.');
        setText('forecastTableLabel', summary.selected_table_label || 'РќРµС‚ С‚Р°Р±Р»РёС†С‹');
        setText('forecastHistoryMode', summary.history_window_label || 'Р’СЃРµ РіРѕРґС‹');
        setText('forecastSliceLabel', summary.slice_label || 'Р’СЃРµ РїРѕР¶Р°СЂС‹');
        setText('forecastTemperatureMode', summary.temperature_scenario_display || 'РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ СЃРµР·РѕРЅРЅРѕСЃС‚СЊ');
        setText('forecastAverageValue', summary.average_probability_display || '0%');
        setText('forecastDaysTotal', (summary.forecast_days_display || '0') + ' РґРЅРµР№');
        setText('forecastHeroPriority', executiveBrief.top_territory_label || risk.top_territory_label || '-');
        setText('forecastHeroPriorityMeta', executiveBrief.priority_reason || risk.top_territory_explanation || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕРїСЂРµРґРµР»РµРЅРёСЏ С‚РµСЂСЂРёС‚РѕСЂРёРё РїРµСЂРІРѕРіРѕ РІРЅРёРјР°РЅРёСЏ.');
        setText('forecastHeroConfidence', executiveBrief.confidence_label || risk.top_territory_confidence_label || leadTerritory.ranking_confidence_label || passport.confidence_label || 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ');
        setText('forecastHeroConfidenceScore', executiveBrief.confidence_score_display || risk.top_territory_confidence_score_display || leadTerritory.ranking_confidence_display || passport.confidence_score_display || '0 / 100');
        setText('forecastHeroConfidenceMeta', executiveBrief.confidence_summary || risk.top_territory_confidence_note || leadTerritory.ranking_confidence_note || passport.validation_summary || 'РџРѕСЏСЃРЅРµРЅРёРµ РїРѕ РЅР°РґРµР¶РЅРѕСЃС‚Рё С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРіРѕ РІС‹РІРѕРґР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.');
        setText('forecastCommandExportExcerpt', executiveBrief.export_excerpt || 'РљСЂР°С‚РєР°СЏ СЌРєСЃРїРѕСЂС‚РёСЂСѓРµРјР°СЏ СЃРїСЂР°РІРєР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.');
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
        setText('forecastSidebarTable', summary.selected_table_label || 'РќРµС‚ С‚Р°Р±Р»РёС†С‹');
        setText('forecastSidebarHistory', summary.history_period_label || 'РќРµС‚ РґР°РЅРЅС‹С…');
        setText('forecastSidebarHorizon', (summary.forecast_days_display || '0') + ' РґРЅ.');
        applyToneClass(byId('forecastHeroPriorityCard'), normalizeTone(executiveBrief.priority_tone || leadTerritory.risk_tone || 'low'));
        applyToneClass(byId('forecastHeroConfidenceCard'), normalizeTone(executiveBrief.confidence_tone || risk.top_territory_confidence_tone || leadTerritory.ranking_confidence_tone || passport.confidence_tone || 'fire'));

        setText('forecastDailyChartTitle', 'Р§С‚Рѕ РѕР¶РёРґР°РµС‚СЃСЏ РїРѕ РґРЅСЏРј');
        setText('forecastWeekdayChartTitle', 'РљР°РєРёРµ РґРЅРё РЅРµРґРµР»Рё С‡Р°С‰Рµ РЅР°РїСЂСЏР¶С‘РЅРЅРµРµ');
        setText('forecastRiskDescription', risk.model_description || '');
        setText('forecastRiskTopLabel', risk.top_territory_label || '-');
        setText('forecastRiskTopExplanation', risk.top_territory_explanation || 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ Р»РёРґРёСЂСѓСЋС‰РµР№ С‚РµСЂСЂРёС‚РѕСЂРёРё.');

        var summaryNode = byId('forecastSummaryLine');
        if (summaryNode) {
            summaryNode.textContent = buildSummaryLine(summary, data);
        }

        renderInsights(data.insights || []);
        renderCommandCards(executiveBrief);
        renderCommandNotes(executiveBrief);
        renderNotes('forecastNotesList', data.notes || [], 'Р—Р°РјРµС‡Р°РЅРёР№ РїРѕРєР° РЅРµС‚.');
        renderNotes('forecastRiskNotes', risk.notes || [], 'РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІСЏС‚СЃСЏ РїСЂРёРјРµС‡Р°РЅРёСЏ Рѕ РіСЂР°РЅРёС†Р°С… РјРµР¶РґСѓ СЃС†РµРЅР°СЂРЅС‹Рј РїСЂРѕРіРЅРѕР·РѕРј, ML-РїСЂРѕРіРЅРѕР·РѕРј Рё С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅС‹Рј РїСЂРёРѕСЂРёС‚РµС‚РѕРј.');
        renderWeightProfile(risk.weight_profile || {});
        renderForecastTable(data.forecast_rows || []);
        renderRiskSummary(risk.summary_cards || []);
        renderRiskTerritories(risk.territories || []);
        renderFeatureCards(risk.feature_cards || data.features || []);
        renderForecastCharts(charts);
        syncForecastStageVisibility(data);
        syncSidebarBadge(data);
        hideForecastError();
        updateForecastBriefExport({
            table_name: filters.table_name || '',
            district: filters.district || 'all',
            cause: filters.cause || 'all',
            object_category: filters.object_category || 'all',
            temperature: filters.temperature || '',
            forecast_days: filters.forecast_days || '',
            history_window: filters.history_window || ''
        });
        updateForecastScreenLinks({
            table_name: filters.table_name || '',
            cause: filters.cause || 'all',
            object_category: filters.object_category || 'all',
            temperature: filters.temperature || '',
            forecast_days: filters.forecast_days || '',
            history_window: filters.history_window || ''
        });
    }

        return {
            applyProgressBars: applyProgressBars,
            applyForecastData: applyForecastData,
            collectForecastFiltersFromForm: collectForecastFiltersFromForm,
            downloadAnalyticalBrief: downloadAnalyticalBrief,
            getCurrentForecastData: function () {
                return currentForecastData;
            },
            hideForecastError: hideForecastError,
            renderForecastJobRuntime: renderForecastJobRuntime,
            setForecastAsyncVisibility: setForecastAsyncVisibility,
            showForecastError: showForecastError,
            syncForecastStageVisibility: syncForecastStageVisibility,
            updateDecisionSupportJobState: updateDecisionSupportJobState,
            updateForecastBriefExport: updateForecastBriefExport,
            updateForecastScreenLinks: updateForecastScreenLinks
        };
    };
})();


