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

    function applyProgressBars(root) {
        var scope = root && typeof root.querySelectorAll === 'function' ? root : document;
        Array.prototype.forEach.call(scope.querySelectorAll('[data-bar-width]'), function (node) {
            node.style.setProperty('--bar-width', normalizePercent(node.getAttribute('data-bar-width'), '0%'));
        });
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

    function renderScenarioQualityCards(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="stat-card">' +
                '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>' +
                '<strong class="stat-value">' + escapeHtml(item.value || '-') + '</strong>' +
                '<span class="stat-foot">' + escapeHtml(item.meta || '') + '</span>' +
            '</article>';
        }).join('');
    }

    function renderScenarioQuality(quality) {
        var safeQuality = quality || {};
        var tableContainer = byId('scenarioQualityTableShell');
        var rows = Array.isArray(safeQuality.comparison_rows) ? safeQuality.comparison_rows : [];

        setText('scenarioQualityTitle', safeQuality.title || 'Оценка качества сценарного прогноза');
        setText('scenarioQualitySubtitle', safeQuality.subtitle || 'После накопления истории здесь появится проверка на истории и сравнение с базовой моделью.');
        renderScenarioQualityCards('scenarioQualityMetrics', safeQuality.metric_cards || [], 'После расчёта здесь появятся метрики качества сценарного прогноза.');
        renderScenarioQualityCards('scenarioQualityMethodology', safeQuality.methodology_items || [], 'Параметры валидации появятся после проверки на истории.');

        if (tableContainer) {
            if (!rows.length) {
                tableContainer.innerHTML = '<div class="mini-empty">Сравнение сценарного прогноза и базовой модели появится после проверки на истории.</div>';
            } else {
                tableContainer.innerHTML = '<table class="forecast-table">' +
                    '<thead><tr><th>Метод</th><th>Роль</th><th>MAE</th><th>RMSE</th><th>SMAPE</th><th>MAE к базовой модели</th><th>Статус</th></tr></thead>' +
                    '<tbody>' + rows.map(function (row) {
                        return '<tr>' +
                            '<td data-label="Метод">' + escapeHtml(row.method_label || '-') + '</td>' +
                            '<td data-label="Роль">' + escapeHtml(row.role_label || '-') + '</td>' +
                            '<td data-label="MAE">' + escapeHtml(row.mae_display || '-') + '</td>' +
                            '<td data-label="RMSE">' + escapeHtml(row.rmse_display || '-') + '</td>' +
                            '<td data-label="SMAPE">' + escapeHtml(row.smape_display || '-') + '</td>' +
                            '<td data-label="MAE к базовой модели">' + escapeHtml(row.mae_delta_display || '—') + '</td>' +
                            '<td data-label="Статус">' + escapeHtml(row.selection_label || '-') + '</td>' +
                        '</tr>';
                    }).join('') + '</tbody></table>';
            }
        }

        renderNotes('scenarioQualityDissertation', safeQuality.dissertation_points || [], 'После расчета здесь появятся формулировки для раздела “оценка качества”.');
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
                    '<td data-label="Дата">' + escapeHtml(row.date_display) + '</td>' +
                    '<td data-label="День недели">' + escapeHtml(row.weekday_label) + '</td>' +
                    '<td data-label="Вероятность пожара">' + escapeHtml(row.fire_probability_display || '0%') + '</td>' +
                    '<td data-label="Комментарий"><span class="forecast-scenario-pill tone-' + escapeHtml(row.scenario_tone || 'sky') + '">' + escapeHtml(row.scenario_label || 'Около обычного') + '</span><div class="forecast-cell-note">' + escapeHtml(row.scenario_hint || '') + '</div></td>' +
                '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return false;
        }

        var figure = chart && chart.plotly;
        if (!figure || !window.Plotly || !Array.isArray(figure.data) || !figure.data.length) {
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            chartNode.innerHTML = '';
            return false;
        }

        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
        return true;
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
                    '<span>Что сделать первым</span>' +
                    '<strong>' + escapeHtml(item.action_label || 'Оставить территорию в плановом наблюдении') + '</strong>' +
                    '<p>' + escapeHtml(item.action_hint || '') + '</p>' +
                '</div>' +
                '<div class="risk-metrics-grid">' + metricsHtml + '</div>' +
                '<div class="risk-components-grid">' + componentsHtml + '</div>' +
                '<p class="risk-formula"><strong>Формула риска:</strong> ' + escapeHtml(item.risk_formula_display || '') + '</p>' +
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
                '<p class="risk-drivers"><strong>Почему территория наверху:</strong> ' + escapeHtml(whyText) + '</p>' +
                '<p class="risk-drivers"><strong>Надёжность вывода:</strong> ' + escapeHtml(reliabilityText) + '</p>' +
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

    function renderMiniCards(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<article class="risk-mini-card">' +
                '<div class="risk-mini-head"><strong>' + escapeHtml(item.label) + '</strong><span>' + escapeHtml(item.risk_display || '') + '</span></div>' +
                '<p>' + escapeHtml(item.meta || '') + '</p>' +
            '</article>';
        }).join('');
    }

    function syncGeoPanel(geo, hasRenderedChart) {
        var panel = byId('forecastGeoPanel');
        var chartNode = byId('forecastGeoChart');
        var compactNode = byId('forecastGeoCompactHint');
        var compactMessage = geo && geo.compact_message ? geo.compact_message : '';
        var shouldCompact = !hasRenderedChart;

        if (panel) {
            panel.classList.toggle('is-compact', shouldCompact);
        }
        if (chartNode) {
            chartNode.classList.toggle('is-hidden', shouldCompact);
        }
        if (compactNode) {
            compactNode.textContent = compactMessage;
            compactNode.classList.toggle('is-hidden', !compactMessage);
        }
    }


    var currentForecastData = window.__FIRE_FORECAST_INITIAL__ || null;
    var forecastRequestToken = 0;

    function buildForecastRequestQuery(baseQuery, includeDecisionSupport) {
        var params = new URLSearchParams(baseQuery || '');
        params.set('include_decision_support', includeDecisionSupport ? '1' : '0');
        return params.toString();
    }

    function setDecisionSupportStatus(message, state) {
        var node = byId('forecastDecisionSupportStatus');
        var text = String(message == null ? '' : message).trim();
        if (!node) {
            return;
        }

        node.textContent = text;
        node.classList.toggle('is-hidden', !text);
        node.classList.remove('is-pending', 'is-ready', 'is-error');
        if (text && state) {
            node.classList.add('is-' + state);
        }
    }

    function syncDecisionSupportStatus(data) {
        if (!data) {
            setDecisionSupportStatus('', '');
            return;
        }

        if (data.decision_support_error) {
            setDecisionSupportStatus(data.decision_support_status_message, 'error');
            return;
        }
        if (data.decision_support_pending) {
            setDecisionSupportStatus(data.decision_support_status_message, 'pending');
            return;
        }
        if (data.decision_support_ready && data.decision_support_status_message) {
            setDecisionSupportStatus(data.decision_support_status_message, 'ready');
            return;
        }
        setDecisionSupportStatus('', '');
    }

    async function requestForecastPayload(query) {
        var response = await fetch('/api/forecasting-data?' + query, { headers: { Accept: 'application/json' } });
        if (!response.ok) {
            throw new Error('fetch failed');
        }
        return response.json();
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

    function renderQualityPassport(passport) {
        var safePassport = passport || {};
        var notes = Array.isArray(safePassport.reliability_notes) ? safePassport.reliability_notes.slice() : [];
        if (Array.isArray(safePassport.critical_gaps) && safePassport.critical_gaps.length) {
            notes.unshift('Нужно усилить: ' + safePassport.critical_gaps.join(', ') + '.');
        }

        setText('forecastQualityScore', safePassport.confidence_score_display || '0 / 100');
        setText('forecastQualityLabel', safePassport.confidence_label || 'Ограниченная');
        setText('forecastQualityTables', safePassport.table_count_display || '0');
        setText('forecastQualityUsed', safePassport.used_count_display || '0');
        setText('forecastQualityPartial', safePassport.partial_count_display || '0');
        setText('forecastQualityMissing', safePassport.missing_count_display || '0');
        setText('forecastQualitySummary', safePassport.validation_summary || 'Паспорт качества появится после расчёта.');
        setText('forecastValidationBadge', safePassport.validation_label || 'Валидация ограничена');
        applyToneClass(byId('forecastQualityScoreCard'), safePassport.confidence_tone || 'fire');
        applyToneClass(byId('forecastValidationBadge'), safePassport.confidence_tone || 'fire');
        renderNotes('forecastQualityNotes', notes, 'Паспорт качества появится после расчёта.');
    }

    function renderWeightProfile(profile) {
        var safeProfile = profile || {};
        var cardsContainer = byId('forecastWeightProfileCards');
        var notes = [];
        var components = Array.isArray(safeProfile.components) ? safeProfile.components : [];

        setText('forecastWeightProfileDescription', safeProfile.description || 'После расчета здесь появится прозрачная схема компонентных весов.');
        setText('forecastWeightModeBadge', safeProfile.status_label || 'Активный профиль');
        applyToneClass(byId('forecastWeightModeBadge'), safeProfile.status_tone || 'forest');

        if (cardsContainer) {
            if (!components.length) {
                cardsContainer.innerHTML = '<div class="mini-empty">Компоненты и веса появятся после расчета.</div>';
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
        renderNotes('forecastWeightProfileNotes', notes, 'После расчета здесь появятся пояснения по профилю весов.');
    }

    function renderHistoricalValidation(validation) {
        var safeValidation = validation || {};
        var cardsContainer = byId('forecastValidationCards');
        var windowsContainer = byId('forecastValidationWindows');
        var metricCards = Array.isArray(safeValidation.metric_cards) ? safeValidation.metric_cards : [];
        var windows = Array.isArray(safeValidation.recent_windows) ? safeValidation.recent_windows : [];

        setText('forecastValidationSummary', safeValidation.summary || 'После расчёта здесь появится заготовка под историческую проверку ранжирования.');
        setText('forecastHistoryValidationBadge', safeValidation.status_label || 'Пока без проверки');
        applyToneClass(byId('forecastHistoryValidationBadge'), safeValidation.status_tone || 'fire');

        if (cardsContainer) {
            if (!metricCards.length) {
                cardsContainer.innerHTML = '<div class="mini-empty">Метрики проверки появятся после расчета.</div>';
            } else {
                cardsContainer.innerHTML = metricCards.map(function (item) {
                    return '<article class="quality-stat-card">' +
                        '<span>' + escapeHtml(item.label || '-') + '</span>' +
                        '<strong>' + escapeHtml(item.value || '-') + '</strong>' +
                        '<small>' + escapeHtml(item.meta || '') + '</small>' +
                    '</article>';
                }).join('');
            }
        }

        if (windowsContainer) {
            if (!windows.length) {
                windowsContainer.innerHTML = '<div class="mini-empty">Исторические окна появятся после расчета.</div>';
            } else {
                windowsContainer.innerHTML = windows.map(function (item) {
                    return '<article class="risk-mini-card">' +
                        '<div class="risk-mini-head"><strong>' + escapeHtml(item.label || '-') + '</strong><span>' + escapeHtml(item.risk_display || '') + '</span></div>' +
                        '<p>' + escapeHtml(item.meta || '') + '</p>' +
                    '</article>';
                }).join('');
            }
        }

        renderNotes('forecastValidationNotes', safeValidation.notes || [], 'После расчёта здесь появятся примечания по исторической проверке ранжирования.');
    }
    function renderCommandCards(brief) {
        var container = byId('forecastCommandCards');
        var cards = brief && Array.isArray(brief.cards) ? brief.cards : [];

        if (!container) {
            return;
        }

        if (!cards.length) {
            container.innerHTML = '<div class="mini-empty">Управленческий бриф появится после расчёта.</div>';
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
        var geo = risk.geo_summary || {};
        var territories = Array.isArray(risk.territories) ? risk.territories : [];
        var weightProfile = risk.weight_profile || {};
        var validation = risk.historical_validation || {};
        var notes = [];
        var seenNotes = {};

        [].concat(passport.reliability_notes || [], weightProfile.notes || [], validation.notes || [], risk.notes || [], data.notes || []).forEach(function (note) {
            var text = String(note || '').trim();
            if (text && !seenNotes[text]) {
                seenNotes[text] = true;
                notes.push(text);
            }
        });

        var lines = [
            'Краткая справка: сценарный прогноз и блок поддержки решений',
            'Сформировано: ' + (data.generated_at || '-'),
            '',
            'Срез анализа',
            'Таблица: ' + (summary.selected_table_label || 'Все таблицы'),
            'История: ' + (summary.history_window_label || 'Все годы'),
            'Срез: ' + (summary.slice_label || 'Все пожары'),
            'Горизонт прогноза: ' + (summary.forecast_days_display || '0') + ' дней',
            '',
            'Оценка качества сценарного прогноза',
            'Статус: ' + (quality.title || 'Оценка качества сценарного прогноза'),
            'Комментарий: ' + (quality.subtitle || 'Недостаточно данных для оценки качества.'),
            '',
            'Профиль весов',
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

        lines.push('', 'Паспорт качества данных');
        lines.push('Статус валидации данных: ' + (passport.validation_label || 'Валидация данных ограничена'));
        lines.push('Надёжность данных: ' + (passport.confidence_label || 'Ограниченная') + ' (' + (passport.confidence_score_display || '0 / 100') + ')');
        lines.push('Комментарий: ' + (passport.validation_summary || 'Оценка качества не сформирована.'));
        lines.push('Надёжность вывода по территории-лидеру: ' + ((risk.top_territory_confidence_label || (territories[0] && territories[0].ranking_confidence_label) || 'Ограниченная')) + ' (' + ((risk.top_territory_confidence_score_display || (territories[0] && territories[0].ranking_confidence_display) || '0 / 100')) + ')');
        lines.push('Пояснение: ' + ((risk.top_territory_confidence_note || (territories[0] && territories[0].ranking_confidence_note) || 'Нет пояснения по надёжности вывода.')));

        lines.push('', 'Историческая проверка ranking-блока');
        lines.push('Статус: ' + (validation.status_label || 'Пока без проверки'));
        lines.push('Комментарий: ' + (validation.summary || 'Нет данных для проверки.'));
        (validation.metric_cards || []).forEach(function (item) {
            lines.push('- ' + (item.label || 'Метрика') + ': ' + (item.value || '-') + ' | ' + (item.meta || ''));
        });

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

        lines.push('', 'Карта риска');
        lines.push('Зона внимания: ' + (geo.top_zone_label || '-'));
        lines.push('Пиковый риск на карте: ' + (geo.top_risk_display || '0 / 100'));
        lines.push('Зон выделено: ' + (geo.hotspots_count_display || '0'));
        lines.push('Пояснение: ' + (geo.top_explanation || 'Нет данных для карты риска.'));

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
        var geo = risk.geo_summary || {};
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
        setText('forecastLeadSummary', executiveBrief.lead || risk.top_territory_explanation || 'После расчёта здесь появится краткое управленческое объяснение.');
        setText('forecastTableLabel', summary.selected_table_label || 'Нет таблицы');
        setText('forecastHistoryMode', summary.history_window_label || 'Все годы');
        setText('forecastSliceLabel', summary.slice_label || 'Все пожары');
        setText('forecastTemperatureMode', summary.temperature_scenario_display || 'Историческая сезонность');
        setText('forecastAverageValue', summary.average_probability_display || '0%');
        setText('forecastDaysTotal', (summary.forecast_days_display || '0') + ' дней');
        setText('forecastHeroPriority', executiveBrief.top_territory_label || risk.top_territory_label || '-');
        setText('forecastHeroPriorityMeta', executiveBrief.priority_reason || risk.top_territory_explanation || 'Недостаточно данных для определения первого приоритета.');
        setText('forecastHeroConfidence', executiveBrief.confidence_label || risk.top_territory_confidence_label || leadTerritory.ranking_confidence_label || passport.confidence_label || 'Ограниченная');
        setText('forecastHeroConfidenceScore', executiveBrief.confidence_score_display || risk.top_territory_confidence_score_display || leadTerritory.ranking_confidence_display || passport.confidence_score_display || '0 / 100');
        setText('forecastHeroConfidenceMeta', executiveBrief.confidence_summary || risk.top_territory_confidence_note || leadTerritory.ranking_confidence_note || passport.validation_summary || 'Пояснение по надёжности вывода появится после расчёта.');
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

        setText('forecastDailyChartTitle', charts.daily ? charts.daily.title : 'Что было и что ожидается');
        setText('forecastWeekdayChartTitle', charts.weekday ? charts.weekday.title : 'В какие дни недели пожары случаются чаще');
        setText('forecastGeoChartTitle', charts.geo ? charts.geo.title : 'Карта зон риска');

        setText('forecastRiskDescription', risk.model_description || '');
        setText('forecastRiskTopLabel', risk.top_territory_label || '-');
        setText('forecastRiskTopExplanation', risk.top_territory_explanation || 'Недостаточно данных для лидирующей территории.');
        setText('forecastGeoDescription', geo.model_description || '');
        setText('forecastGeoCoverage', geo.coverage_display || '0 с координатами');
        setText('forecastGeoTopZone', geo.top_zone_label || '-');
        setText('forecastGeoTopRisk', geo.top_risk_display || '0 / 100');
        setText('forecastGeoHotspotsCount', geo.hotspots_count_display || '0');
        setText('forecastGeoTopExplanation', geo.top_explanation || 'Нет данных для объяснения зоны риска.');
        setText('forecastGeoCompactHint', geo.compact_message || '');

        var summaryNode = byId('forecastSummaryLine');
        if (summaryNode) {
            summaryNode.textContent =
                (summary.slice_label || 'Все пожары') +
                ' | Средняя вероятность: ' + (summary.average_probability_display || '0%') +
                ' | Максимум: ' + (summary.peak_forecast_probability_display || '0%') + ' (' + (summary.peak_forecast_day_display || '-') + ')' +
                ' | К последним 4 неделям: ' + (summary.forecast_vs_recent_display || '0%');
        }

        renderScenarioQuality(data.quality_assessment || {});
        renderInsights(data.insights || []);
        renderCommandCards(executiveBrief);
        renderCommandNotes(executiveBrief);
        renderNotes('forecastNotesList', data.notes || [], 'Замечаний пока нет.');
        renderNotes('forecastRiskNotes', risk.notes || [], 'После расчёта здесь появятся примечания по блоку поддержки решений.');
        renderQualityPassport(passport);
        renderWeightProfile(risk.weight_profile || {});
        renderHistoricalValidation(risk.historical_validation || {});
        renderForecastTable(data.forecast_rows || []);
        renderRiskSummary(risk.summary_cards || []);
        renderRiskTerritories(risk.territories || []);
        renderFeatureCards(risk.feature_cards || data.features || []);
        renderMiniCards('forecastGeoHotspots', geo.hotspots || [], 'Зоны появятся после расчёта.');
        renderMiniCards('forecastGeoDistricts', geo.districts || [], 'Сводка по районам появится после расчёта.');
        renderChart(charts.daily, 'forecastDailyChart', 'forecastDailyChartFallback');
        renderChart(charts.weekday, 'forecastWeekdayChart', 'forecastWeekdayChartFallback');
        var geoRendered = renderChart(charts.geo, 'forecastGeoChart', 'forecastGeoChartFallback');
        syncGeoPanel(geo, geoRendered);
        syncDecisionSupportStatus(data);
        updateForecastBriefExport({
            table_name: filters.table_name || '',
            district: filters.district || 'all',
            cause: filters.cause || 'all',
            object_category: filters.object_category || 'all',
            temperature: filters.temperature || '',
            forecast_days: filters.forecast_days || '',
            history_window: filters.history_window || ''
        });
    }

    async function fetchDecisionSupport(baseQuery, requestToken) {
        try {
            var data = await requestForecastPayload(buildForecastRequestQuery(baseQuery, true));
            if (requestToken !== forecastRequestToken) {
                return;
            }
            applyForecastData(data);
        } catch (error) {
            if (requestToken !== forecastRequestToken) {
                return;
            }
            console.error(error);
            setDecisionSupportStatus('Не удалось догрузить блок приоритетов территорий. Базовый сценарный прогноз уже показан.', 'error');
        }
    }

    async function fetchForecastData() {
        var form = byId('forecastForm');
        var button = byId('forecastRefreshButton');
        if (!form) {
            return;
        }

        var requestToken = forecastRequestToken + 1;
        var baseQuery = new URLSearchParams(new FormData(form)).toString();
        var query = buildForecastRequestQuery(baseQuery, false);
        forecastRequestToken = requestToken;
        if (button) {
            button.disabled = true;
        }
        setDecisionSupportStatus('Собираем базовый сценарный прогноз...', 'pending');

        try {
            var data = await requestForecastPayload(query);
            if (requestToken !== forecastRequestToken) {
                return;
            }
            applyForecastData(data);
            window.history.replaceState({}, '', baseQuery ? '/forecasting?' + baseQuery : '/forecasting');
            if (data.decision_support_pending) {
                void fetchDecisionSupport(baseQuery, requestToken);
            }
        } catch (error) {
            console.error(error);
            form.submit();
        } finally {
            if (button && requestToken === forecastRequestToken) {
                button.disabled = false;
            }
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        var form = byId('forecastForm');
        var initialData = window.__FIRE_FORECAST_INITIAL__ || null;
        var syncBriefLink = function () {
            updateForecastBriefExport(collectForecastFiltersFromForm());
        };

        applyProgressBars(document);
        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchForecastData();
            });

            Array.prototype.forEach.call(form.querySelectorAll('select, input'), function (field) {
                field.addEventListener('change', syncBriefLink);
                if (field.tagName === 'INPUT') {
                    field.addEventListener('input', syncBriefLink);
                }
            });
        }

        syncBriefLink();
        if (initialData && initialData.bootstrap_mode !== 'deferred') {
            applyForecastData(initialData);
            if (initialData.decision_support_pending && form) {
                forecastRequestToken += 1;
                void fetchDecisionSupport(new URLSearchParams(new FormData(form)).toString(), forecastRequestToken);
            }
        } else {
            fetchForecastData();
        }
    });
})();
