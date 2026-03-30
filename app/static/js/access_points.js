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
            node.textContent = value == null ? '' : String(value);
        }
    }

    function setSelectOptions(id, options, selectedValue, emptyLabel) {
        var node = byId(id);
        if (!node) {
            return;
        }
        var safeOptions = Array.isArray(options) && options.length ? options : [{ value: '', label: emptyLabel }];
        node.innerHTML = safeOptions.map(function (option) {
            var selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        }).join('');
    }

    function setAsyncVisible(visible) {
        var node = byId('accessPointsAsyncState');
        if (node) {
            node.classList.toggle('is-hidden', !visible);
        }
    }

    function showLoading(message) {
        var loadingNode = byId('accessPointsLoadingState');
        var errorNode = byId('accessPointsErrorState');
        setAsyncVisible(true);
        setText('accessPointsLoadingLead', 'Готовим рейтинг проблемных точек');
        setText('accessPointsLoadingMessage', message || 'Собираем incidents по точкам, считаем score доступности и объяснения.');
        if (loadingNode) {
            loadingNode.classList.remove('is-hidden', 'is-ready');
            loadingNode.classList.add('is-pending');
        }
        if (errorNode) {
            errorNode.classList.add('is-hidden');
        }
    }

    function hideLoading() {
        var loadingNode = byId('accessPointsLoadingState');
        if (loadingNode) {
            loadingNode.classList.add('is-hidden');
            loadingNode.classList.remove('is-pending');
        }
        setAsyncVisible(false);
    }

    function showError(message) {
        var loadingNode = byId('accessPointsLoadingState');
        var errorNode = byId('accessPointsErrorState');
        setAsyncVisible(true);
        if (loadingNode) {
            loadingNode.classList.add('is-hidden');
        }
        setText('accessPointsErrorMessage', message || 'Не удалось обновить рейтинг. Попробуйте повторить запрос.');
        if (errorNode) {
            errorNode.classList.remove('is-hidden');
        }
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
        setText('accessPointsLead', data.top_point_explanation || 'Недостаточно данных для выделения приоритетных точек.');
        setText('accessPointsTableLabel', summary.selected_table_label || 'Все таблицы');
        setText('accessPointsDistrictLabel', summary.selected_district_label || 'Все районы');
        setText('accessPointsYearLabel', summary.selected_year_label || 'Все годы');
        setText('accessPointsLimitLabel', summary.limit_display || '25');
        setText('accessPointsHeroLabel', data.top_point_label || '-');
        setText(
            'accessPointsHeroMeta',
            (summary.top_point_priority_label || 'Нет оценки') + ' | score ' + (summary.top_point_score_display || '0')
        );
        setText('accessPointsIncompleteCount', summary.incomplete_points_display || '0');
    }

    function renderSummaryLine(data) {
        var summary = data.summary || {};
        var line = (summary.filter_description || 'Срез не выбран')
            + ' | точек в рейтинге: ' + (summary.total_points_display || '0')
            + ' | критичный приоритет: ' + (summary.critical_points_display || '0');
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

    function renderChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var figure = chart && chart.plotly;
        if (!window.Plotly || !figure || !Array.isArray(figure.data) || !figure.data.length) {
            chartNode.innerHTML = '';
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            return;
        }

        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
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
            var chips = [];
            chips.push('<span class="point-chip">' + escapeHtml(point.typology_label || 'Комбинированный риск') + '</span>');
            chips.push('<span class="point-chip">Пожаров: ' + escapeHtml(point.incident_count_display || '0') + '</span>');
            chips.push('<span class="point-chip">Полнота: ' + escapeHtml(point.completeness_display || '0%') + '</span>');
            (Array.isArray(point.reason_chips) ? point.reason_chips : []).slice(0, 2).forEach(function (reason) {
                chips.push('<span class="point-chip point-chip-soft">' + escapeHtml(reason) + '</span>');
            });
            return ''
                + '<article class="point-card tone-' + escapeHtml(point.tone || 'normal') + '">'
                + '<div class="point-card-head"><span class="point-rank">#' + escapeHtml(point.rank || '') + '</span><span class="point-score">' + escapeHtml(point.score_display || '0') + '</span></div>'
                + '<strong class="point-label">' + escapeHtml(point.label || '-') + '</strong>'
                + '<span class="point-meta">' + escapeHtml(point.entity_type || 'Точка') + (point.location_hint ? ' | ' + escapeHtml(point.location_hint) : '') + '</span>'
                + '<div class="point-chip-row">' + chips.join('') + '</div>'
                + '<p class="point-explanation">' + escapeHtml(point.explanation || '') + '</p>'
                + '</article>';
        }).join('');
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
            + '<th>#</th><th>Точка</th><th>Тип</th><th>Район</th><th>Score</th><th>Типология</th><th>Пожары</th><th>Удалённость</th><th>Прибытие</th><th>Вода</th><th>Полнота</th><th>Почему в топе</th>'
            + '</tr></thead>'
            + '<tbody>'
            + points.map(function (point) {
                return ''
                    + '<tr>'
                    + '<td>' + escapeHtml(point.rank || '') + '</td>'
                    + '<td><strong>' + escapeHtml(point.label || '-') + '</strong>'
                    + (point.coordinates_display ? '<div class="table-subnote">' + escapeHtml(point.coordinates_display) + '</div>' : '')
                    + '</td>'
                    + '<td>' + escapeHtml(point.entity_type || '—') + '</td>'
                    + '<td>' + escapeHtml(point.district || '—') + '</td>'
                    + '<td><span class="score-pill tone-' + escapeHtml(point.tone || 'normal') + '">' + escapeHtml(point.score_display || '0') + '</span></td>'
                    + '<td>' + escapeHtml(point.typology_label || 'Комбинированный риск') + '</td>'
                    + '<td>' + escapeHtml(point.incident_count_display || '0') + '</td>'
                    + '<td>' + escapeHtml(point.average_distance_display || 'н/д') + '</td>'
                    + '<td>' + escapeHtml(point.average_response_display || 'н/д') + '</td>'
                    + '<td>нет воды: ' + escapeHtml(point.no_water_share_display || '0%') + '<br>пропуски: ' + escapeHtml(point.water_unknown_share_display || '0%') + '</td>'
                    + '<td>' + escapeHtml(point.completeness_display || '0%') + '</td>'
                    + '<td>' + escapeHtml(point.explanation || '') + '</td>'
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
            return ''
                + '<article class="incomplete-card">'
                + '<strong>' + escapeHtml(point.label || '-') + '</strong>'
                + '<span>' + escapeHtml(point.incomplete_note || 'Нужна проверка полноты данных.') + '</span>'
                + '<span>Investigation score: ' + escapeHtml(point.investigation_score_display || '0') + ' | Полнота: ' + escapeHtml(point.completeness_display || '0%') + '</span>'
                + '<span>' + escapeHtml(point.explanation || '') + '</span>'
                + '</article>';
        }).join('');
    }

    function renderNotes(data) {
        var node = byId('accessPointsNotes');
        var notes = Array.isArray(data.notes) ? data.notes : [];
        if (!node) {
            return;
        }
        if (!notes.length) {
            node.innerHTML = '<li>Здесь появятся короткие пояснения по качеству данных и смыслу рейтинга.</li>';
            return;
        }
        node.innerHTML = notes.map(function (note) {
            return '<li>' + escapeHtml(note) + '</li>';
        }).join('');
    }

    function renderFilters(data) {
        var filters = data.filters || {};
        setSelectOptions('accessPointsTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('accessPointsDistrictFilter', filters.available_districts, filters.district, 'Все районы');
        setSelectOptions('accessPointsYearFilter', filters.available_years, filters.year, 'Все годы');
        setSelectOptions('accessPointsLimitFilter', filters.available_limits, filters.limit, 'Top 25');
    }

    function render(data) {
        var charts = data.charts || {};
        renderFilters(data);
        renderSidebarStatus(data);
        renderHero(data);
        renderSummaryLine(data);
        renderCards(data);
        setText('accessPointsScatterTitle', charts.scatter ? charts.scatter.title : 'Проблемные точки на проекции доступности и последствий');
        renderChart(charts.scatter, 'accessPointsScatterChart', 'accessPointsScatterChartFallback');
        renderTopPoints(data);
        renderRankingTable(data);
        renderTypology(data);
        renderIncomplete(data);
        renderNotes(data);
        hideLoading();
    }

    function getFormParams() {
        return {
            table_name: (byId('accessPointsTableFilter') || {}).value || 'all',
            district: (byId('accessPointsDistrictFilter') || {}).value || 'all',
            year: (byId('accessPointsYearFilter') || {}).value || 'all',
            limit: (byId('accessPointsLimitFilter') || {}).value || '25'
        };
    }

    function updateUrl(params) {
        var query = new URLSearchParams(params);
        window.history.replaceState({}, '', '/access-points?' + query.toString());
    }

    function decodeErrorMessage(payload, fallback) {
        if (payload && payload.error && payload.error.message) {
            return payload.error.message;
        }
        return fallback;
    }

    var currentController = null;

    async function fetchAccessPoints(params) {
        if (currentController) {
            currentController.abort();
        }
        var controller = new AbortController();
        currentController = controller;
        var refreshButton = byId('accessPointsRefreshButton');
        if (refreshButton) {
            refreshButton.disabled = true;
        }
        showLoading('Собираем incidents по точкам, считаем score доступности и объяснения.');
        try {
            var query = new URLSearchParams(params);
            var response = await fetch('/api/access-points-data?' + query.toString(), {
                headers: { Accept: 'application/json' },
                signal: controller.signal
            });
            var payload = await response.json();
            if (!response.ok || payload.ok === false) {
                throw new Error(decodeErrorMessage(payload, 'Не удалось построить рейтинг проблемных точек.'));
            }
            render(payload);
            updateUrl(params);
        } catch (error) {
            if (error && error.name === 'AbortError') {
                return;
            }
            showError(error && error.message ? error.message : 'Не удалось построить рейтинг проблемных точек.');
        } finally {
            if (currentController === controller) {
                currentController = null;
            }
            if (refreshButton && currentController === null) {
                refreshButton.disabled = false;
            }
        }
    }

    var initialData = window.__FIRE_ACCESS_POINTS_INITIAL__ || {};
    render(initialData);

    var form = byId('accessPointsForm');
    if (form) {
        form.addEventListener('submit', function (event) {
            event.preventDefault();
            fetchAccessPoints(getFormParams());
        });
    }

    var retryButton = byId('accessPointsRetryButton');
    if (retryButton) {
        retryButton.addEventListener('click', function () {
            fetchAccessPoints(getFormParams());
        });
    }

    if (initialData.bootstrap_mode === 'deferred') {
        fetchAccessPoints(getFormParams());
    }
})();
