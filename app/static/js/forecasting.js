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
            container.innerHTML = '<div class="mini-empty">После расчета здесь появится decision-support ранжирование территорий.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            var components = Array.isArray(item.component_scores) ? item.component_scores : [];
            var recommendations = Array.isArray(item.recommendations) ? item.recommendations : [];
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
                    '<div class="risk-component-bar"><span style="width: ' + escapeHtml(component.bar_width || '12%') + '"></span></div>' +
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
                        '</div>' +
                    '</div>' +
                    '<div class="risk-territory-score">' + escapeHtml(item.risk_display || '0 / 100') + '</div>' +
                '</div>' +
                '<div class="risk-score-bar"><span style="width: ' + escapeHtml(item.bar_width || '10%') + '"></span></div>' +
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
                    '<span>Среднее прибытие: <strong>' + escapeHtml(item.response_time_display || 'Нет данных') + '</strong></span>' +
                    '<span>Удалённость от ПЧ: <strong>' + escapeHtml(item.distance_display || 'Нет данных') + '</strong></span>' +
                    '<span>Вода: <strong>' + escapeHtml(item.water_supply_display || 'Нет данных') + '</strong></span>' +
                    '<span>Объекты: <strong>' + escapeHtml(item.dominant_object_category || 'Не указано') + '</strong></span>' +
                '</div>' +
                '<p class="risk-drivers"><strong>Почему территория наверху:</strong> ' + escapeHtml(item.drivers_display || '') + '</p>' +
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

    function applyToneClass(node, tone) {
        if (!node) {
            return;
        }

        node.className = node.className.replace(/\btone-[a-z]+\b/g, '').replace(/\s+/g, ' ').trim();
        if (tone) {
            node.className += (node.className ? ' ' : '') + 'tone-' + tone;
        }
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
                        '<div class="risk-weight-head"><strong>' + escapeHtml(item.label || 'Компонент') + '</strong><span>' + escapeHtml(item.weight_display || '0%') + '</span></div>' +
                        '<p>' + escapeHtml(item.description || '') + '</p>' +
                        '<div class="risk-weight-meta">' +
                            '<span>База: <strong>' + escapeHtml(item.weight_display || '0%') + '</strong></span>' +
                            '<span>Село: <strong>' + escapeHtml(item.rural_weight_display || '0%') + '</strong></span>' +
                            '<span>Сдвиг: <strong>' + escapeHtml(item.rural_shift_display || '0 п.п.') + '</strong></span>' +
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

        setText('forecastValidationSummary', safeValidation.summary || 'После расчета здесь появится заготовка под историческую проверку ranking.');
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

        renderNotes('forecastValidationNotes', safeValidation.notes || [], 'После расчета здесь появятся примечания по исторической проверке ranking.');
    }
    function buildAnalyticalBrief(data) {
        var summary = data.summary || {};
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
            'Краткая справка: scenario forecast и блок поддержки решений',
            'Сформировано: ' + (data.generated_at || '-'),
            '',
            'Срез анализа',
            'Таблица: ' + (summary.selected_table_label || 'Все таблицы'),
            'История: ' + (summary.history_window_label || 'Все годы'),
            'Срез: ' + (summary.slice_label || 'Все пожары'),
            'Горизонт прогноза: ' + (summary.forecast_days_display || '0') + ' дней',
            '',
            'Профиль весов',
            'Режим: ' + (weightProfile.mode_label || 'Экспертные веса'),
            'Описание: ' + (weightProfile.description || 'Нет описания.'),
        ];

        if (Array.isArray(weightProfile.components) && weightProfile.components.length) {
            weightProfile.components.forEach(function (item) {
                lines.push('- ' + (item.label || 'Компонент') + ': база ' + (item.weight_display || '0%') + ', село ' + (item.rural_weight_display || '0%') + ', сдвиг ' + (item.rural_shift_display || '0 п.п.'));
            });
        }

        lines.push('', 'Паспорт качества данных');
        lines.push('Статус валидации данных: ' + (passport.validation_label || 'Валидация данных ограничена'));
        lines.push('Надёжность вывода: ' + (passport.confidence_label || 'Ограниченная') + ' (' + (passport.confidence_score_display || '0 / 100') + ')');
        lines.push('Комментарий: ' + (passport.validation_summary || 'Оценка качества не сформирована.'));

        lines.push('', 'Черновая историческая проверка ranking');
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
                (item.component_scores || []).forEach(function (component) {
                    lines.push('   - ' + (component.label || 'Компонент') + ': ' + (component.score_display || '0 / 100') + ', вес ' + (component.weight_display || '0%') + ', вклад ' + (component.contribution_display || '0 балла'));
                });
                lines.push('   Почему: ' + (item.drivers_display || 'Нет пояснения.'));
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
        var geo = risk.geo_summary || {};
        var passport = risk.quality_passport || {};

        currentForecastData = data;

        setSelectOptions('forecastTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('forecastHistoryWindowFilter', filters.available_history_windows, filters.history_window, 'Все годы');
        setSelectOptions('forecastDistrictFilter', filters.available_districts, filters.district, 'Все районы');
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

        renderInsights(data.insights || []);
        renderNotes('forecastNotesList', data.notes || [], 'Замечаний пока нет.');
        renderNotes('forecastRiskNotes', risk.notes || [], 'После расчета здесь появятся примечания по блоку поддержки решений.');
        renderQualityPassport(passport);
        renderWeightProfile(risk.weight_profile || {});
        renderHistoricalValidation(risk.historical_validation || {});
        renderForecastTable(data.forecast_rows || []);
        renderRiskSummary(risk.summary_cards || []);
        renderRiskTerritories(risk.territories || []);
        renderFeatureCards(risk.feature_cards || data.features || []);
        renderMiniCards('forecastGeoHotspots', geo.hotspots || [], 'Зоны появятся после расчета.');
        renderMiniCards('forecastGeoDistricts', geo.districts || [], 'Сводка по районам появится после расчета.');
        renderChart(charts.daily, 'forecastDailyChart', 'forecastDailyChartFallback');
        renderChart(charts.weekday, 'forecastWeekdayChart', 'forecastWeekdayChartFallback');
        var geoRendered = renderChart(charts.geo, 'forecastGeoChart', 'forecastGeoChartFallback');
        syncGeoPanel(geo, geoRendered);
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
        var exportButton = byId('forecastExportBriefButton');
        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchForecastData();
            });
        }

        if (exportButton) {
            exportButton.addEventListener('click', downloadAnalyticalBrief);
        }

        if (window.__FIRE_FORECAST_INITIAL__) {
            applyForecastData(window.__FIRE_FORECAST_INITIAL__);
        }
    });
})();

