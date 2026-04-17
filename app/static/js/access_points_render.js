(function (global) {
    var shared = global.FireUi;
    if (!shared) {
        return;
    }

    global.AccessPointsRender = {
        create: function createAccessPointsRender() {
            var uiHelpers = global.FireUiHelpers || {};
            var byId = shared.byId;
            var escapeHtml = shared.escapeHtml;
            var renderChart = shared.renderPlotlyFigure;
            var renderListItems = shared.renderListItems;
            var setSectionHidden = shared.setSectionHidden;
            var setSelectOptions = shared.setSelectOptions;
            var setText = shared.setText;
            var setHidden = typeof uiHelpers.setHidden === 'function'
                ? uiHelpers.setHidden
                : function (nodeOrId, hidden) {
                    var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
                    if (!node) {
                        return;
                    }
                    node.classList.toggle('is-hidden', !!hidden);
                };

function showLoading(message) {
        var loadingNode = byId('accessPointsLoadingState');
        var errorNode = byId('accessPointsErrorState');
        setSectionHidden('accessPointsAsyncState', false);
        setText('accessPointsLoadingLead', 'Готовим рейтинг проблемных точек');
        setText(
            'accessPointsLoadingMessage',
            message || 'Собираем incidents по точкам, считаем explainable score, причины попадания в топ и uncertainty notes.'
        );
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-ready');
            loadingNode.classList.add('is-pending');
        }
        setHidden(errorNode, true);
    }

    function hideLoading() {
        var loadingNode = byId('accessPointsLoadingState');
        if (loadingNode) {
            loadingNode.classList.add('is-hidden');
            loadingNode.classList.remove('is-pending');
        }
        setSectionHidden('accessPointsAsyncState', true);
    }

    function showError(message) {
        var loadingNode = byId('accessPointsLoadingState');
        var errorNode = byId('accessPointsErrorState');
        setSectionHidden('accessPointsAsyncState', false);
        if (loadingNode) {
            loadingNode.classList.add('is-hidden');
        }
        setText('accessPointsErrorMessage', message || 'Не удалось обновить рейтинг. Попробуйте повторить запрос.');
        setHidden(errorNode, false);
    }

    function renderSidebarStatus(data) {
        var summary = data.summary || {};
        var node = byId('accessPointsSidebarStatus');
        if (!node) {
            return;
        }
        var badgeClass = 'status-badge';
        if (data.bootstrap_mode === 'deferred' || data.has_data) {
            badgeClass += ' status-badge-live';
        }
        var badgeLabel = data.bootstrap_mode === 'deferred'
            ? 'Собираем ranking'
            : (data.has_data ? 'Рейтинг готов' : 'Нужны данные');
        node.innerHTML = ''
            + '<span class="' + badgeClass + '">' + escapeHtml(badgeLabel) + '</span>'
            + '<div class="status-line"><span>Таблица</span><strong>' + escapeHtml(summary.selected_table_label || 'Все таблицы') + '</strong></div>'
            + '<div class="status-line"><span>Точек в срезе</span><strong>' + escapeHtml(summary.total_points_display || '0') + '</strong></div>'
            + '<div class="status-line"><span>Топ-1</span><strong>' + escapeHtml(summary.top_point_label || '-') + '</strong></div>';
    }

    function renderHero(data) {
        var summary = data.summary || {};
        var severityBand = summary.top_point_severity_band || 'нет оценки';
        setText('accessPointsLead', data.top_point_explanation || 'Недостаточно данных для выделения приоритетных точек.');
        setText('accessPointsTableLabel', summary.selected_table_label || 'Все таблицы');
        setText('accessPointsDistrictLabel', summary.selected_district_label || 'Все районы');
        setText('accessPointsYearLabel', summary.selected_year_label || 'Все годы');
        setText('accessPointsLimitLabel', summary.limit_display || '25');
        setText('accessPointsHeroLabel', data.top_point_label || '-');
        setText(
            'accessPointsHeroMeta',
            severityBand + ' риск | score ' + (summary.top_point_score_display || '0')
        );
        setText('accessPointsIncompleteCount', summary.uncertainty_points_display || summary.incomplete_points_display || '0');
    }

    function renderSummaryLine(data) {
        var summary = data.summary || {};
        var line = (summary.filter_description || 'Срез не выбран')
            + ' | точек в рейтинге: ' + (summary.total_points_display || '0')
            + ' | высокий и выше: ' + (summary.high_points_display || summary.review_points_display || '0')
            + ' | критический: ' + (summary.critical_points_display || '0');
        setText('accessPointsSummaryLine', line);
    }

    function renderCards(data) {
        var node = byId('accessPointsCards');
        var cards = Array.isArray(data.summary_cards) ? data.summary_cards : [];
        if (!node) {
            return;
        }
        if (!cards.length) {
            node.innerHTML = '<article class="stat-card"><span class="stat-label">Сводка</span><strong class="stat-value">0</strong><span class="stat-foot">Карточки появятся после расчёта.</span></article>';
            return;
        }
        node.innerHTML = cards.map(function (card) {
            var className = 'stat-card';
            if (card.tone === 'critical') {
                className += ' stat-card-accent';
            }
            return ''
                + '<article class="' + className + '">'
                + '<span class="stat-label">' + escapeHtml(card.label || '') + '</span>'
                + '<strong class="stat-value">' + escapeHtml(card.value || '0') + '</strong>'
                + '<span class="stat-foot">' + escapeHtml(card.meta || '') + '</span>'
                + '</article>';
        }).join('');
    }

    function buildReasonBadges(point) {
        var chips = [];
        if (point.severity_band) {
            chips.push('<span class="point-chip">' + escapeHtml(point.severity_band) + '</span>');
        }
        if (point.typology_label) {
            chips.push('<span class="point-chip">' + escapeHtml(point.typology_label) + '</span>');
        }
        chips.push('<span class="point-chip">Пожаров: ' + escapeHtml(point.incident_count_display || '0') + '</span>');
        chips.push('<span class="point-chip">Полнота: ' + escapeHtml(point.completeness_display || '0%') + '</span>');
        (Array.isArray(point.reason_chips) ? point.reason_chips : []).slice(0, 2).forEach(function (reason) {
            chips.push('<span class="point-chip point-chip-soft">' + escapeHtml(reason) + '</span>');
        });
        if (!chips.length) {
            chips.push('<span class="point-chip">Точка</span>');
        }
        return chips.join('');
    }

    function renderTopPoints(data) {
        var node = byId('accessPointsTopList');
        var points = Array.isArray(data.top_points) ? data.top_points : [];
        if (!node) {
            return;
        }
        if (!points.length) {
            node.innerHTML = '<div class="mini-empty">После расчёта здесь появятся самые проблемные точки.</div>';
            return;
        }
        node.innerHTML = points.map(function (point) {
            var explanation = point.human_readable_explanation || point.explanation || '';
            return ''
                + '<article class="point-card tone-' + escapeHtml(point.tone || 'normal') + '">'
                + '<div class="point-card-head"><span class="point-rank">#' + escapeHtml(point.rank || '') + '</span><span class="point-score">' + escapeHtml(point.total_score_display || point.score_display || '0') + '</span></div>'
                + '<strong class="point-label">' + escapeHtml(point.label || '-') + '</strong>'
                + '<span class="point-meta">' + escapeHtml(point.entity_type || 'Точка') + (point.location_hint ? ' | ' + escapeHtml(point.location_hint) : '') + '</span>'
                + '<div class="point-chip-row">' + buildReasonBadges(point) + '</div>'
                + '<p class="point-explanation">' + escapeHtml(explanation) + '</p>'
                + '</article>';
        }).join('');
    }

    function formatReasonDetails(point) {
        var details = Array.isArray(point.reason_details) ? point.reason_details : [];
        if (details.length) {
            return details.slice(0, 3).map(function (detail) {
                var label = detail.label || detail.code || '';
                var contribution = detail.contribution_display || '';
                return escapeHtml(label + ' ' + contribution);
            }).join('<br>');
        }
        var codes = Array.isArray(point.top_reason_codes) ? point.top_reason_codes : [];
        if (codes.length) {
            return escapeHtml(codes.slice(0, 3).join(', '));
        }
        return '-';
    }

    function renderRankingTable(data) {
        var node = byId('accessPointsRankingTableShell');
        var points = Array.isArray(data.points) ? data.points : [];
        if (!node) {
            return;
        }
        if (!points.length) {
            node.innerHTML = '<div class="mini-empty">После расчёта здесь появится ranking отдельных проблемных точек.</div>';
            return;
        }
        node.innerHTML = ''
            + '<table class="data-table table-sticky-first access-points-table">'
            + '<thead><tr>'
            + '<th>#</th><th>Точка</th><th>Тип</th><th>Район</th><th>Score</th><th>Band</th><th>Типология</th><th>Пожары</th><th>Удалённость</th><th>Прибытие</th><th>Вода</th><th>Полнота</th><th>Драйверы</th><th>Почему в топе</th>'
            + '</tr></thead>'
            + '<tbody>'
            + points.map(function (point) {
                var explanation = point.human_readable_explanation || point.explanation || '';
                return ''
                    + '<tr>'
                    + '<td>' + escapeHtml(point.rank || '') + '</td>'
                    + '<td><strong>' + escapeHtml(point.label || '-') + '</strong>'
                    + (point.coordinates_display ? '<div class="table-subnote">' + escapeHtml(point.coordinates_display) + '</div>' : '')
                    + '</td>'
                    + '<td>' + escapeHtml(point.entity_type || '-') + '</td>'
                    + '<td>' + escapeHtml(point.district || '-') + '</td>'
                    + '<td><span class="score-pill tone-' + escapeHtml(point.tone || 'normal') + '">' + escapeHtml(point.total_score_display || point.score_display || '0') + '</span></td>'
                    + '<td>' + escapeHtml(point.severity_band || '-') + '</td>'
                    + '<td>' + escapeHtml(point.typology_label || 'Комбинированный риск') + '</td>'
                    + '<td>' + escapeHtml(point.incident_count_display || '0') + '</td>'
                    + '<td>' + escapeHtml(point.average_distance_display || 'н/д') + '</td>'
                    + '<td>' + escapeHtml(point.average_response_display || 'н/д') + '</td>'
                    + '<td>нет воды: ' + escapeHtml(point.no_water_share_display || '0%') + '<br>пропуски: ' + escapeHtml(point.water_unknown_share_display || '0%') + '</td>'
                    + '<td>' + escapeHtml(point.completeness_display || '0%') + '</td>'
                    + '<td>' + formatReasonDetails(point) + '</td>'
                    + '<td>' + escapeHtml(explanation) + '</td>'
                    + '</tr>';
            }).join('')
            + '</tbody></table>';
    }

    function renderTypology(data) {
        var node = byId('accessPointsTypologyShell');
        var typology = Array.isArray(data.typology) ? data.typology : [];
        if (!node) {
            return;
        }
        if (!typology.length) {
            node.innerHTML = '<div class="mini-empty">Типология появится после расчёта рейтинга.</div>';
            return;
        }
        node.innerHTML = typology.map(function (item) {
            return ''
                + '<article class="typology-card">'
                + '<strong>' + escapeHtml(item.label || '') + '</strong>'
                + '<span>' + escapeHtml(item.count_display || '0') + ' точек | ' + escapeHtml(item.share_display || '0%') + '</span>'
                + '<span>Максимальный score: ' + escapeHtml(item.max_score_display || '0') + '</span>'
                + '<span>Лидер: ' + escapeHtml(item.lead_label || '-') + '</span>'
                + '</article>';
        }).join('');
    }

    function renderScoreDistribution(data) {
        var node = byId('accessPointsDistributionShell');
        var distribution = data.score_distribution || {};
        var bands = Array.isArray(distribution.bands) ? distribution.bands : [];
        var buckets = Array.isArray(distribution.buckets) ? distribution.buckets : [];
        if (!node) {
            return;
        }
        if (!bands.length && !buckets.length) {
            node.innerHTML = '<div class="mini-empty">Распределение балла риска появится после расчёта рейтинга.</div>';
            return;
        }
        var cards = [
            ''
                + '<article class="score-distribution-card score-distribution-card-metric">'
                + '<span class="score-distribution-label">Средний балл</span>'
                + '<strong class="score-distribution-value">' + escapeHtml(distribution.average_score_display || '0') + '</strong>'
                + '<span class="score-distribution-meta">По всем точкам в рейтинге</span>'
                + '</article>',
            ''
                + '<article class="score-distribution-card score-distribution-card-metric">'
                + '<span class="score-distribution-label">Медианный балл</span>'
                + '<strong class="score-distribution-value">' + escapeHtml(distribution.median_score_display || '0') + '</strong>'
                + '<span class="score-distribution-meta">Типичное значение без влияния выбросов</span>'
                + '</article>'
        ];
        bands.forEach(function (item) {
            cards.push(
                '<article class="score-distribution-card">'
                + '<span class="score-distribution-label">' + escapeHtml(item.label || '') + ' риск</span>'
                + '<strong class="score-distribution-value">' + escapeHtml(item.count_display || '0') + '</strong>'
                + '<span class="score-distribution-meta">' + escapeHtml(item.share_display || '0%') + ' от выборки</span>'
                + '</article>'
            );
        });
        buckets.forEach(function (item) {
            cards.push(
                '<article class="score-distribution-card score-distribution-card-range">'
                + '<span class="score-distribution-label">Диапазон ' + escapeHtml(item.label || '') + '</span>'
                + '<strong class="score-distribution-value">' + escapeHtml(item.count_display || '0') + '</strong>'
                + '<span class="score-distribution-meta">точек</span>'
                + '</article>'
            );
        });
        node.innerHTML = cards.join('');
    }

    function renderReasonBreakdown(data) {
        var node = byId('accessPointsReasonBreakdownShell');
        var items = Array.isArray(data.reason_breakdown) ? data.reason_breakdown : [];
        if (!node) {
            return;
        }
        if (!items.length) {
            node.innerHTML = '<div class="mini-empty">Сводка по драйверам появится после расчёта рейтинга.</div>';
            return;
        }
        node.innerHTML = items.map(function (item) {
            return ''
                + '<article class="reason-breakdown-card">'
                + '<span class="reason-breakdown-label">' + escapeHtml(item.label || '') + '</span>'
                + '<strong class="reason-breakdown-value">' + escapeHtml(item.count_display || '0') + '</strong>'
                + '<span class="reason-breakdown-meta">' + escapeHtml(item.share_display || '0%') + ' точек попадают под этот фактор</span>'
                + '<span class="reason-breakdown-meta">В среднем добавляет ' + escapeHtml(item.average_contribution_display || '0') + ' балла</span>'
                + '<span class="reason-breakdown-foot">Характерный пример: ' + escapeHtml(item.lead_label || '-') + '</span>'
                + '</article>';
        }).join('');
    }

    function renderIncomplete(data) {
        var node = byId('accessPointsIncompleteShell');
        var points = Array.isArray(data.incomplete_points) ? data.incomplete_points : [];
        if (!node) {
            return;
        }
        if (!points.length) {
            node.innerHTML = '<div class="mini-empty">Отдельные точки с риском из-за пропусков пока не выделены.</div>';
            return;
        }
        node.innerHTML = points.map(function (point) {
            var explanation = point.human_readable_explanation || point.explanation || '';
            return ''
                + '<article class="incomplete-card">'
                + '<strong>' + escapeHtml(point.label || '-') + '</strong>'
                + '<span>' + escapeHtml(point.incomplete_note || 'Нужна проверка полноты данных.') + '</span>'
                + '<span>Investigation score: ' + escapeHtml(point.investigation_score_display || '0') + ' | Полнота: ' + escapeHtml(point.completeness_display || '0%') + '</span>'
                + '<span>' + escapeHtml(explanation) + '</span>'
                + '</article>';
        }).join('');
    }

    function renderFeaturePicker(filters) {
        var container = byId('accessPointsFeaturePicker');
        if (!container) {
            return;
        }

        var items = Array.isArray(filters.available_features) ? filters.available_features : [];
        var selectedValues = Array.isArray(filters.feature_columns)
            ? filters.feature_columns.map(function (item) { return String(item); })
            : [];
        var body;

        if (items.length) {
            body = '<div class="access-point-feature-grid">' + items.map(function (feature) {
                var checked = feature.is_selected || selectedValues.indexOf(String(feature.name)) >= 0 ? ' checked' : '';
                return ''
                    + '<label class="access-point-feature-option">'
                    + '<input type="checkbox" name="feature_columns" value="' + escapeHtml(feature.name) + '"' + checked + '>'
                    + '<span class="access-point-feature-copy">'
                    + '<strong class="access-point-feature-name">' + escapeHtml(feature.label || feature.name || '') + '</strong>'
                    + '<span class="access-point-feature-meta">' + escapeHtml(feature.description || '')
                    + ' Заполненность: ' + escapeHtml(feature.coverage_display || '0%')
                    + ' | Дисперсия: ' + escapeHtml(feature.variance_display || '0') + '</span>'
                    + '</span>'
                    + '</label>';
            }).join('') + '</div>';
        } else {
            body = '<div class="mini-empty">После загрузки данных здесь появятся explainable-факторы, которые можно включать и исключать из scoring-модели проблемных точек.</div>'
                + selectedValues.map(function (value) {
                    return '<input type="hidden" name="feature_columns" value="' + escapeHtml(value) + '">';
                }).join('');
        }

        container.innerHTML = ''
            + '<span>Факторы explainable-score</span>'
            + body
            + '<span class="access-point-feature-help">Выбранные факторы напрямую участвуют в итоговом access risk score, разложении по причинам и в ранжировании top-N.</span>';
    }

    function renderFilters(data) {
        var filters = data.filters || {};
        setSelectOptions('accessPointsTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('accessPointsDistrictFilter', filters.available_districts, filters.district, 'Все районы');
        setSelectOptions('accessPointsYearFilter', filters.available_years, filters.year, 'Все годы');
        setSelectOptions('accessPointsLimitFilter', filters.available_limits, filters.limit, 'Top 25');
        renderFeaturePicker(filters);
    }
    function renderCharts(charts) {
        setText(
            'accessPointsScatterTitle',
            charts.scatter ? charts.scatter.title : 'Проблемные точки на двумерной проекции риска'
        );
        renderChart(charts.scatter, 'accessPointsScatterChart', 'accessPointsScatterChartFallback');
        renderChart(charts.factor_heatmap, 'accessPointsHeatmapChart', 'accessPointsHeatmapFallback');
        renderChart(charts.factor_bar, 'accessPointsFactorBarChart', 'accessPointsFactorBarFallback');
        renderChart(charts.score_histogram, 'accessPointsHistogramChart', 'accessPointsHistogramChartFallback');
    }
    function render(data) {
        var charts = data.charts || {};
        renderFilters(data);
        renderSidebarStatus(data);
        renderHero(data);
        renderSummaryLine(data);
        renderCards(data);
        renderCharts(charts);
        renderTopPoints(data);
        renderRankingTable(data);
        renderTypology(data);
        renderScoreDistribution(data);
        renderReasonBreakdown(data);
        renderIncomplete(data);
        renderListItems('accessPointsUncertaintyNotes', data.uncertainty_notes, 'Здесь появятся пояснения по uncertainty penalty и low support.');
        renderListItems('accessPointsNotes', data.notes, 'Здесь появятся короткие пояснения по качеству данных и смыслу рейтинга.');
        hideLoading();
    }

            return {
                hideLoading: hideLoading,
                render: render,
                showError: showError,
                showLoading: showLoading
            };
        }
    };
}(window));




