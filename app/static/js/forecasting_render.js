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
            container.innerHTML = '<div class="mini-empty">После расчета здесь появятся ближайшие даты и вероятность пожара по сценарию.</div>';
            return;
        }

        container.innerHTML = '<table class="forecast-table">' +
            '<thead><tr><th>Дата</th><th>День недели</th><th>Вероятность пожара</th><th>Комментарий</th></tr></thead>' +
            '<tbody>' + rows.map(function (row) {
                return '<tr>' +
                    '<td data-label="Дата">' + escapeHtml(row.date_display) + '</td>' +
                    '<td data-label="День недели">' + escapeHtml(row.weekday_label) + '</td>' +
                    '<td data-label="Вероятность пожара">' + escapeHtml(row.fire_probability_display || '0%') + '</td>' +
                    '<td data-label="Комментарий"><span class="forecast-scenario-pill tone-' + escapeHtml(row.scenario_tone || 'sky') + '">' + escapeHtml(row.scenario_label || 'Около обычного') + '</span><div class="forecast-cell-note">' + escapeHtml(row.scenario_hint || '') + '</div></td>' +
                '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderRiskSummary(items) {
        var container = byId('forecastRiskCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Карточки блока поддержки решений появятся после расчета.</div>';
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
            container.innerHTML = '<div class="mini-empty">После расчёта здесь появится ранжирование территорий для поддержки решений.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            var components = Array.isArray(item.component_scores) ? item.component_scores : [];
            var recommendations = Array.isArray(item.recommendations) ? item.recommendations : [];
            var rankingTone = normalizeTone(item.ranking_confidence_tone || 'fire');
            var whyText = item.ranking_reason || item.drivers_display || 'Недостаточно данных для объяснения приоритета.';
            var reliabilityText = item.ranking_confidence_note || 'Оценка надёжности появится после расчёта.';
            var metricOrder = [
                { key: 'fire_frequency', fallback: 'Частота пожаров' },
                { key: 'consequence_severity', fallback: 'Тяжесть последствий' },
                { key: 'long_arrival_risk', fallback: 'Долгое прибытие' },
                { key: 'water_supply_deficit', fallback: 'Дефицит воды' }
            ];

            var metricsHtml = metricOrder.map(function (descriptor) {
                var component = findComponent(item, descriptor.key);
                return '<div><span>' + escapeHtml(component ? component.label : descriptor.fallback) + '</span><strong>' + escapeHtml(component ? component.score_display : '0 / 100') + '</strong></div>';
            }).join('');

            var componentsHtml = components.map(function (component) {
                return '<article class="risk-component-card tone-' + escapeHtml(component.tone || 'low') + '">' +
                    '<div class="risk-component-head"><strong>' + escapeHtml(component.label || 'Компонент') + '</strong><span>' + escapeHtml(component.score_display || '0 / 100') + '</span></div>' +
                    '<div class="risk-component-bar"><span data-bar-width="' + escapeHtml(component.bar_width || '12%') + '"></span></div>' +
                    '<div class="risk-component-meta">' + escapeHtml(component.summary || '') + '</div>' +
                    '<p>' + escapeHtml(component.rationale || '') + '</p>' +
                '</article>';
            }).join('');

            var recommendationsHtml = recommendations.length ? recommendations.map(function (recommendation) {
                return '<article class="risk-recommendation-item">' +
                    '<strong>' + escapeHtml(recommendation.label || 'Рекомендация') + '</strong>' +
                    '<span>' + escapeHtml(recommendation.detail || '') + '</span>' +
                '</article>';
            }).join('') : '<div class="mini-empty">Рекомендации появятся после расчета.</div>';

            return '<article class="risk-territory-card tone-' + escapeHtml(item.risk_tone || 'low') + '">' +
                '<div class="risk-territory-head">' +
                    '<div>' +
                        '<strong>' + escapeHtml(item.label) + '</strong>' +
                        '<div class="risk-territory-tags">' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.risk_tone || 'low') + '">' + escapeHtml(item.risk_class_label || 'Низкий риск') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(item.priority_tone || 'sky') + '">' + escapeHtml(item.priority_label || 'Плановое наблюдение') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-sky">' + escapeHtml(item.weight_mode_label || 'Экспертные веса') + '</span>' +
                            '<span class="forecast-badge risk-badge tone-' + escapeHtml(rankingTone) + '">' + escapeHtml(item.ranking_confidence_label || 'Ограниченная') + '</span>' +
                        '</div>' +
                    '</div>' +
                    '<div class="risk-territory-score">' + escapeHtml(item.risk_display || '0 / 100') + '</div>' +
                '</div>' +
                '<div class="risk-score-bar"><span data-bar-width="' + escapeHtml(item.bar_width || '10%') + '"></span></div>' +
                '<div class="risk-territory-callout">' +
                    '<span>Что проверить первым</span>' +
                    '<strong>' + escapeHtml(item.action_label || 'Оставить территорию в плановом наблюдении') + '</strong>' +
                    '<p>' + escapeHtml(item.action_hint || '') + '</p>' +
                '</div>' +
                '<div class="risk-metrics-grid">' + metricsHtml + '</div>' +
                '<div class="risk-components-grid">' + componentsHtml + '</div>' +
                '<p class="risk-formula"><strong>Как сложился итоговый балл:</strong> ' + escapeHtml(item.risk_formula_display || '') + '</p>' +
                '<div class="risk-recommendation-list">' + recommendationsHtml + '</div>' +
                '<div class="risk-territory-meta">' +
                    '<span>Контекст: <strong>' + escapeHtml(item.settlement_context_label || 'Не указан') + '</strong></span>' +
                    '<span>Последний пожар: <strong>' + escapeHtml(item.last_fire_display || '-') + '</strong></span>' +
                    '<span>Travel-time: <strong>' + escapeHtml(item.travel_time_display || 'н/д') + '</strong></span>' +
                    '<span>Среднее прибытие: <strong>' + escapeHtml(item.response_time_display || 'Нет данных') + '</strong></span>' +
                    '<span>Удалённость от ПЧ: <strong>' + escapeHtml(item.distance_display || 'Нет данных') + '</strong></span>' +
                    '<span>Покрытие ПЧ: <strong>' + escapeHtml(item.fire_station_coverage_display || 'н/д') + ' (' + escapeHtml(item.fire_station_coverage_label || 'нет данных') + ')</strong></span>' +
                    '<span>Сервисная зона: <strong>' + escapeHtml(item.service_zone_label || 'не определена') + '</strong></span>' +
                    '<span>Логистический приоритет: <strong>' + escapeHtml(item.logistics_priority_display || '0 / 100') + '</strong></span>' +
                    '<span>Вода: <strong>' + escapeHtml(item.water_supply_display || 'Нет данных') + '</strong></span>' +
                    '<span>Объекты: <strong>' + escapeHtml(item.dominant_object_category || 'Не указано') + '</strong></span>' +
                '</div>' +
                '<p class="risk-drivers"><strong>Почему именно эта территория:</strong> ' + escapeHtml(whyText) + '</p>' +
                '<p class="risk-drivers"><strong>Почему уровень доверия такой:</strong> ' + escapeHtml(reliabilityText) + '</p>' +
            '</article>';
        }).join('');
    }
    function renderFeatureCards(items) {
        var container = byId('forecastFeatureCards');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Список признаков появится после расчета.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="forecast-feature-card status-' + escapeHtml(item.status || 'missing') + '">' +
                '<div class="forecast-feature-head">' +
                    '<strong>' + escapeHtml(item.label) + '</strong>' +
                    '<span class="forecast-badge">' + escapeHtml(item.status_label || 'Не найдена') + '</span>' +
                '</div>' +
                '<p>' + escapeHtml(item.description || '') + '</p>' +
                '<div class="forecast-feature-source">' + escapeHtml(item.source || 'Не найдена') + '</div>' +
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
        setText('forecastErrorMessage', message || 'Не удалось пересчитать прогноз. Попробуйте еще раз.');
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
            node.textContent = 'Подготавливаем прогноз';
            node.classList.add('status-badge-live');
            return;
        }
        if (data && data.has_data) {
            node.textContent = 'Сценарный прогноз собран';
            node.classList.add('status-badge-live');
            return;
        }
        node.textContent = 'Нужно уточнить фильтры';
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
        return 'Сейчас показано: ' + (safeSummary.slice_label || 'Все пожары') +
            ' | Типичный день: ' + (safeSummary.average_probability_display || '0%') +
            ' | Пик: ' + (safeSummary.peak_forecast_probability_display || '0%') + ' (' + (safeSummary.peak_forecast_day_display || '-') + ')' +
            ' | К последним 4 неделям: ' + (safeSummary.forecast_vs_recent_display || '0%');
    }

    function clearForecastJobRuntime(runtimeNode, titleNode, metaNode, logsNode) {
        runtimeNode.classList.add('is-hidden');
        runtimeNode.classList.remove('is-ready');
        titleNode.textContent = 'Готовим блок поддержки решений';
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
            return 'Подключаем уже запущенный расчёт';
        }
        if (meta.stage_label) {
            return String(meta.stage_label);
        }
        if (safeJob.status === 'pending') {
            return 'Готовим блок поддержки решений';
        }
        return 'Собираем блок поддержки решений';
    }

    function getForecastJobRuntimeMeta(jobPayload) {
        var safeJob = jobPayload || {};
        var meta = safeJob.meta || {};
        var metaParts = [];

        if (meta.stage_message) {
            metaParts.push(String(meta.stage_message));
        }
        if (safeJob.reused) {
            metaParts.push('используем уже запущенный расчёт');
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
        logsNode.textContent = logs.length ? logs.join('\n') : 'Покажем прогресс, как только расчёт перейдёт к следующему этапу.';
        return;
        /* Legacy technical runtime rendering removed.
        if (meta.cache_hit) {
            metaParts.push('кэш');
        }
        if (safeJob.reused) {
            metaParts.push('переиспользован');
        }
        metaNode.textContent = metaParts.join(' | ');
        logsNode.textContent = logs.length ? logs.join('\n') : 'Логи появятся после запуска фоновой задачи.';
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

        setText('forecastWeightProfileDescription', safeProfile.description || 'После расчета здесь появится понятное объяснение, какие факторы сильнее всего двигают территорию вверх или вниз.');
        setText('forecastWeightModeBadge', safeProfile.status_label || 'Активный профиль');
        applyToneClass(byId('forecastWeightModeBadge'), safeProfile.status_tone || 'forest');

        if (cardsContainer) {
            if (!components.length) {
                cardsContainer.innerHTML = '<div class="mini-empty">После расчета здесь появится список факторов, которые больше всего влияют на итоговый балл.</div>';
            } else {
                cardsContainer.innerHTML = components.map(function (item) {
                    return '<article class="risk-weight-card">' +
                        '<div class="risk-weight-head"><strong>' + escapeHtml(item.label || 'Компонент') + '</strong><span>' + escapeHtml(item.current_weight_display || item.weight_display || '0%') + '</span></div>' +
                        '<p>' + escapeHtml(item.description || '') + '</p>' +
                        '<div class="risk-weight-meta">' +
                            '<span>Эксперт: <strong>' + escapeHtml(item.expert_weight_display || item.weight_display || '0%') + '</strong></span>' +
                            '<span>Текущий: <strong>' + escapeHtml(item.current_weight_display || item.weight_display || '0%') + '</strong></span>' +
                            '<span>Калибровка: <strong>' + escapeHtml(item.calibration_shift_display || '0 п.п.') + '</strong></span>' +
                            '<span>Сельский контур: <strong>' + escapeHtml(item.rural_weight_display || item.weight_display || '0%') + '</strong></span>' +
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
        renderNotes('forecastWeightProfileNotes', notes, 'После расчета здесь появятся пояснения, почему профиль весов выглядит именно так.');
    }

    function renderCommandCards(brief) {
        var container = byId('forecastCommandCards');
        var cards = brief && Array.isArray(brief.cards) ? brief.cards : [];

        if (!container) {
            return;
        }

        if (!cards.length) {
            container.innerHTML = '<div class="mini-empty">Короткий вывод появится после расчёта.</div>';
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
        renderNotes('forecastCommandNotes', notes, 'Ограничения и примечания появятся после расчёта.');
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
            'Краткая справка: сценарный прогноз и поддержка решений',
            'Сформировано: ' + (data.generated_at || '-'),
            '',
            'Срез анализа',
            'Таблица: ' + (summary.selected_table_label || 'Все таблицы'),
            'История: ' + (summary.history_window_label || 'Все годы'),
            'Срез: ' + (summary.slice_label || 'Все пожары'),
            'Горизонт прогноза: ' + (summary.forecast_days_display || '0') + ' дней',
            '',
            'Насколько прогноз по дням попадает в историю',
            'Статус: ' + (quality.title || 'Проверка на истории'),
            'Комментарий: ' + (quality.subtitle || 'Недостаточно данных для оценки качества.'),
            '',
            'Почему территория поднялась вверх в приоритете',
            'Режим: ' + (weightProfile.mode_label || 'Экспертные веса'),
            'Описание: ' + (weightProfile.description || 'Нет описания.'),
        ];

        (quality.metric_cards || []).forEach(function (item) {
            lines.push('- ' + (item.label || 'Метрика') + ': ' + (item.value || '-') + ' | ' + (item.meta || ''));
        });
        (quality.dissertation_points || []).forEach(function (item) {
            lines.push('- ' + item);
        });

        if (Array.isArray(weightProfile.components) && weightProfile.components.length) {
            weightProfile.components.forEach(function (item) {
                lines.push('- ' + (item.label || 'Компонент') + ': эксперт ' + (item.expert_weight_display || item.weight_display || '0%') + ', текущий ' + (item.current_weight_display || item.weight_display || '0%') + ', калибровка ' + (item.calibration_shift_display || '0 п.п.') + ', сельский контур ' + (item.rural_weight_display || item.weight_display || '0%'));
            });
        }

        lines.push('Надёжность вывода по территории-лидеру: ' + ((risk.top_territory_confidence_label || (territories[0] && territories[0].ranking_confidence_label) || 'Ограниченная')) + ' (' + ((risk.top_territory_confidence_score_display || (territories[0] && territories[0].ranking_confidence_display) || '0 / 100')) + ')');
        lines.push('Пояснение: ' + ((risk.top_territory_confidence_note || (territories[0] && territories[0].ranking_confidence_note) || 'Нет пояснения по надёжности вывода.')));

        lines.push('', 'Приоритетные территории');
        if (territories.length) {
            territories.slice(0, 5).forEach(function (item, index) {
                lines.push((index + 1) + '. ' + (item.label || 'Территория'));
                lines.push('   Риск: ' + (item.risk_display || '0 / 100') + ' | Класс: ' + (item.risk_class_label || '-') + ' | Приоритет: ' + (item.priority_label || '-'));
                lines.push('   Формула: ' + (item.risk_formula_display || 'Нет формулы.'));
                lines.push('   Логистика: travel-time ' + (item.travel_time_display || 'н/д') + ', покрытие ПЧ ' + (item.fire_station_coverage_display || 'н/д') + ', сервисная зона ' + (item.service_zone_label || 'не определена') + ', логистический приоритет ' + (item.logistics_priority_display || '0 / 100') + '.');
                (item.component_scores || []).forEach(function (component) {
                    lines.push('   - ' + (component.label || 'Компонент') + ': ' + (component.score_display || '0 / 100') + ', вес ' + (component.weight_display || '0%') + ', вклад ' + (component.contribution_display || '0 балла'));
                });
                lines.push('   Почему: ' + (item.ranking_reason || item.drivers_display || 'Нет пояснения.'));
                lines.push('   Надёжность: ' + ((item.ranking_confidence_label || 'Ограниченная')) + ' (' + (item.ranking_confidence_display || '0 / 100') + ')');
                lines.push('   Пояснение: ' + (item.ranking_confidence_note || 'Нет пояснения по надёжности.'));
                lines.push('   Что сделать первым: ' + (item.action_label || 'Плановое наблюдение') + '. ' + (item.action_hint || ''));
            });
        } else {
            lines.push('Нет данных для ранжирования территорий.');
        }

        lines.push('', 'Ограничения и замечания');
        if (notes.length) {
            notes.slice(0, 10).forEach(function (note, index) {
                lines.push((index + 1) + '. ' + note);
            });
        } else {
            lines.push('1. Существенных ограничений в текущем срезе не зафиксировано.');
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

        setSelectOptions('forecastTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('forecastHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Все годы');
        setSelectOptions('forecastDistrictFilter', filters.available_districts, filters.district, 'Все районы');
        setSelectOptions('forecastCauseFilter', filters.available_causes, filters.cause, 'Все причины');
        setSelectOptions('forecastObjectCategoryFilter', filters.available_object_categories, filters.object_category, 'Все категории');
        setSelectOptions('forecastDaysFilter', filters.available_forecast_days, filters.forecast_days, '14 дней');
        setValue('forecastTemperatureInput', filters.temperature || '');

        setText('forecastModelDescription', data.model_description || '');
        setText('forecastLeadSummary', summary.hero_summary || executiveBrief.lead || risk.top_territory_explanation || 'После расчёта здесь появится краткий вывод по датам, где сценарий выглядит напряжённее.');
        setText('forecastTableLabel', summary.selected_table_label || 'Нет таблицы');
        setText('forecastHistoryMode', summary.history_window_label || 'Все годы');
        setText('forecastSliceLabel', summary.slice_label || 'Все пожары');
        setText('forecastTemperatureMode', summary.temperature_scenario_display || 'Историческая сезонность');
        setText('forecastAverageValue', summary.average_probability_display || '0%');
        setText('forecastDaysTotal', (summary.forecast_days_display || '0') + ' дней');
        setText('forecastHeroPriority', executiveBrief.top_territory_label || risk.top_territory_label || '-');
        setText('forecastHeroPriorityMeta', executiveBrief.priority_reason || risk.top_territory_explanation || 'Недостаточно данных для определения территории первого внимания.');
        setText('forecastHeroConfidence', executiveBrief.confidence_label || risk.top_territory_confidence_label || leadTerritory.ranking_confidence_label || passport.confidence_label || 'Ограниченная');
        setText('forecastHeroConfidenceScore', executiveBrief.confidence_score_display || risk.top_territory_confidence_score_display || leadTerritory.ranking_confidence_display || passport.confidence_score_display || '0 / 100');
        setText('forecastHeroConfidenceMeta', executiveBrief.confidence_summary || risk.top_territory_confidence_note || leadTerritory.ranking_confidence_note || passport.validation_summary || 'Пояснение по надежности территориального вывода появится после расчета.');
        setText('forecastCommandExportExcerpt', executiveBrief.export_excerpt || 'Краткая экспортируемая справка появится после расчёта.');
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
        applyToneClass(byId('forecastHeroPriorityCard'), normalizeTone(executiveBrief.priority_tone || leadTerritory.risk_tone || 'low'));
        applyToneClass(byId('forecastHeroConfidenceCard'), normalizeTone(executiveBrief.confidence_tone || risk.top_territory_confidence_tone || leadTerritory.ranking_confidence_tone || passport.confidence_tone || 'fire'));

        setText('forecastDailyChartTitle', 'Что ожидается по дням');
        setText('forecastWeekdayChartTitle', 'Какие дни недели чаще напряжённее');
        setText('forecastRiskDescription', risk.model_description || '');
        setText('forecastRiskTopLabel', risk.top_territory_label || '-');
        setText('forecastRiskTopExplanation', risk.top_territory_explanation || 'Недостаточно данных для лидирующей территории.');

        var summaryNode = byId('forecastSummaryLine');
        if (summaryNode) {
            summaryNode.textContent = buildSummaryLine(summary, data);
        }

        renderInsights(data.insights || []);
        renderCommandCards(executiveBrief);
        renderCommandNotes(executiveBrief);
        renderNotes('forecastNotesList', data.notes || [], 'Замечаний пока нет.');
        renderNotes('forecastRiskNotes', risk.notes || [], 'После расчёта здесь появятся примечания о границах между сценарным прогнозом, ML-прогнозом и территориальным приоритетом.');
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


