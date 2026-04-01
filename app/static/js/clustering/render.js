// Rendering

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

    function renderSidebarStatus(data) {
        var container = byId('clusteringSidebarStatus');
        if (!container) {
            return;
        }

        var summary = data.summary || {};
        var isLoaded = Boolean(data && data.has_data);
        var isDeferred = data && data.bootstrap_mode === 'deferred';
        var badgeClass = 'status-badge';
        if (isLoaded) {
            badgeClass += ' status-badge-live';
        }

        var badgeLabel = isDeferred
            ? 'Подготовка типологии'
            : (isLoaded ? 'Типы территорий рассчитаны' : 'Нужны агрегированные признаки');

        container.innerHTML = ''
            + '<span class="' + badgeClass + '">' + escapeHtml(badgeLabel) + '</span>'
            + '<div class="status-line"><span>Таблица</span><strong>' + escapeHtml(summary.selected_table_label || 'Нет таблицы') + '</strong></div>'
            + '<div class="status-line"><span>Территорий</span><strong>' + escapeHtml(summary.clustered_entities_display || '0') + '</strong></div>'
            + '<div class="status-line"><span>Силуэт</span><strong>' + escapeHtml(summary.silhouette_display || '—') + '</strong></div>';
    }

    function renderHero(data) {
        var summary = data.summary || {};
        setText(
            'clusteringModelDescription',
            data.model_description || 'Выберите таблицу, чтобы собрать агрегированный профиль территории и выделить типы риска.'
        );

        var heroTags = byId('clusteringHeroTags');
        if (heroTags) {
            heroTags.innerHTML = ''
                + '<span class="hero-tag">Таблица: <strong>' + escapeHtml(summary.selected_table_label || 'Нет таблицы') + '</strong></span>'
                + '<span class="hero-tag">Пожаров в истории: <strong>' + escapeHtml(summary.total_incidents_display || '0') + '</strong></span>'
                + '<span class="hero-tag">Территорий в модели: <strong>' + escapeHtml(summary.clustered_entities_display || '0') + '</strong></span>';
        }

        var heroStats = byId('clusteringHeroStats');
        if (heroStats) {
            heroStats.innerHTML = ''
                + '<article class="hero-stat-card">'
                + '<span class="hero-stat-label">' + escapeHtml(summary.suggested_cluster_count_label || 'Рекомендуемый k') + '</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.suggested_cluster_count_display || '—') + '</strong>'
                + '<span class="hero-stat-foot">' + escapeHtml(summary.suggested_cluster_count_note || 'Диагностика k появится, когда хватит данных для сравнения нескольких вариантов.') + '</span>'
                + '</article>'
                + '<article class="hero-stat-card hero-stat-card-soft">'
                + '<span class="hero-stat-label">Точка локтя</span>'
                + '<strong class="hero-stat-value">' + escapeHtml(summary.elbow_cluster_count_display || '—') + '</strong>'
                + '<span class="hero-stat-foot">Где кривая inertia начинает заметно ломаться.</span>'
                + '</article>';
        }
    }

    function renderFeaturePicker(filters) {
        var container = byId('clusteringFeaturePicker');
        if (!container) {
            return;
        }

        var items = Array.isArray(filters.available_features) ? filters.available_features : [];
        var selectedValues = Array.isArray(filters.feature_columns)
            ? filters.feature_columns.map(function (item) { return String(item); })
            : [];

        var body;
        if (items.length) {
            body = '<div class="cluster-feature-grid">' + items.map(function (feature) {
                var checked = feature.is_selected || selectedValues.indexOf(String(feature.name)) >= 0 ? ' checked' : '';
                return ''
                    + '<label class="cluster-feature-option">'
                    + '<input type="checkbox" name="feature_columns" value="' + escapeHtml(feature.name) + '"' + checked + '>'
                    + '<span class="cluster-feature-copy">'
                    + '<strong class="cluster-feature-name">' + escapeHtml(feature.name) + '</strong>'
                    + '<span class="cluster-feature-meta">' + escapeHtml(feature.description || '')
                    + ' Заполненность: ' + escapeHtml(feature.coverage_display || '0%')
                    + ' | Дисперсия: ' + escapeHtml(feature.variance_display || '0') + '</span>'
                    + '</span>'
                    + '</label>';
            }).join('') + '</div>';
        } else {
            body = '<div class="mini-empty">После выбора таблицы здесь появятся агрегированные признаки для типологии территорий риска.</div>';
        }

        container.innerHTML = ''
            + '<span>Агрегированные признаки территории</span>'
            + body
            + '<span class="cluster-feature-help">Базовый набор уже ориентирован на территориальный риск: частота, площадь, ночные пожары, прибытие, последствия и подтвержденность водоснабжения.</span>';
    }

    function renderFilterSummary(summary) {
        var container = byId('clusteringFilterSummary');
        if (!container) {
            return;
        }

        container.textContent = 'Пожаров в истории: ' + (summary.total_incidents_display || '0')
            + ' | Территорий после агрегации: ' + (summary.total_entities_display || '0')
            + ' | В выборке: ' + (summary.sampled_entities_display || '0')
            + ' | Стратегия: ' + (summary.sampling_strategy_label || 'Не выбрана');
    }

    function renderSummaryCards(summary) {
        var container = byId('clusteringStatsGrid');
        if (!container) {
            return;
        }

        container.innerHTML = ''
            + '<article class="stat-card stat-card-accent">'
            + '<span class="stat-label">Пожаров в истории</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.total_incidents_display || '0') + '</strong>'
            + '<span class="stat-foot">Все инциденты, которые вошли в территориальные агрегаты.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Территорий в расчете</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.clustered_entities_display || '0') + '</strong>'
            + '<span class="stat-foot">После отбора по заполненности выбранных признаков.</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Число кластеров</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.cluster_count_display || '0') + '</strong>'
            + '<span class="stat-foot">' + escapeHtml(summary.cluster_count_note || ('Сейчас основной вывод показан для k=' + (summary.cluster_count_display || '0') + '.')) + '</span>'
            + '</article>'
            + '<article class="stat-card">'
            + '<span class="stat-label">Инерция</span>'
            + '<strong class="stat-value">' + escapeHtml(summary.inertia_display || '0') + '</strong>'
            + '<span class="stat-foot">Внутрикластерная компактность после стандартизации агрегатов.</span>'
            + '</article>';
    }

    function renderMetricCards(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return ''
                + '<article class="stat-card">'
                + '<span class="stat-label">' + escapeHtml(item.label || '-') + '</span>'
                + '<strong class="stat-value">' + escapeHtml(item.value || '-') + '</strong>'
                + '<span class="stat-foot">' + escapeHtml(item.meta || '') + '</span>'
                + '</article>';
        }).join('');
    }

    function renderQualityTable(rows) {
        var container = byId('clusteringQualityTableShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">Сравнение алгоритмов появится после расчета.</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="data-table table-stack-mobile">'
            + '<thead><tr><th>Метод</th><th>Силуэт</th><th>Индекс Дэвиса-Болдина</th><th>Индекс Калински-Харабаза</th><th>Баланс</th><th>Статус</th></tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                return ''
                    + '<tr>'
                    + '<td data-label="Метод">' + escapeHtml(row.method_label || '-') + '</td>'
                    + '<td data-label="Силуэт">' + escapeHtml(row.silhouette_display || '-') + '</td>'
                    + '<td data-label="Индекс Дэвиса-Болдина">' + escapeHtml(row.davies_display || '-') + '</td>'
                    + '<td data-label="Индекс Калински-Харабаза">' + escapeHtml(row.calinski_display || '-') + '</td>'
                    + '<td data-label="Баланс">' + escapeHtml(row.balance_display || '-') + '</td>'
                    + '<td data-label="Статус">' + escapeHtml(row.selection_label || '-') + '</td>'
                    + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderNoticeList(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<li>' + escapeHtml(emptyMessage) + '</li>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return '<li>' + escapeHtml(item) + '</li>';
        }).join('');
    }

    function renderDataTable(containerId, columns, rows, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(columns) || !columns.length || !Array.isArray(rows) || !rows.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage) + '</div>';
            return;
        }

        container.innerHTML = ''
            + '<table class="data-table table-sticky-first">'
            + '<thead><tr>' + columns.map(function (column) {
                return '<th>' + escapeHtml(column) + '</th>';
            }).join('') + '</tr></thead>'
            + '<tbody>' + rows.map(function (row) {
                var cells = Array.isArray(row) ? row : [];
                return '<tr>' + cells.map(function (cell) {
                    return '<td>' + escapeHtml(cell) + '</td>';
                }).join('') + '</tr>';
            }).join('') + '</tbody></table>';
    }

    function renderProfiles(items) {
        var container = byId('clusterProfilesShell');
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">Профили типов территорий появятся после расчета.</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return ''
                + '<article class="cluster-overview-card tone-' + escapeHtml(item.tone || 'sky') + '">'
                + '<div class="cluster-overview-head">'
                + '<strong>' + escapeHtml(item.cluster_label || 'Кластер') + '</strong>'
                + '<span class="cluster-badge">' + escapeHtml(item.share_display || '0%') + '</span>'
                + '</div>'
                + '<span class="cluster-overview-meta">' + escapeHtml(item.segment_title || '') + '</span>'
                + '<span class="cluster-overview-meta">Территорий: <span class="cluster-overview-value">' + escapeHtml(item.size_display || '0') + '</span> | Пожаров в истории: <span class="cluster-overview-value">' + escapeHtml(item.incidents_display || '0') + '</span></span>'
                + '<span class="cluster-overview-foot">' + escapeHtml(item.summary || '') + '</span>'
                + '</article>';
        }).join('');
    }


    function applyClusteringData(data) {
        if (!data) {
            return;
        }

        var summary = data.summary || {};
        var filters = data.filters || {};
        var quality = data.quality_assessment || {};
        var charts = data.charts || {};

        renderSidebarStatus(data);
        renderHero(data);

        setSelectOptions('clusterTableFilter', filters.available_tables, filters.table_name, 'Нет таблиц');
        setSelectOptions('clusterCountFilter', filters.available_cluster_counts, filters.cluster_count, '4 кластера');
        setSelectOptions('clusterSampleLimitFilter', filters.available_sample_limits, filters.sample_limit, 'до 1000 территорий');
        setSelectOptions('clusterSamplingStrategyFilter', filters.available_sampling_strategies, filters.sampling_strategy, 'Стратифицированная');
        renderFeaturePicker(filters);
        renderFilterSummary(summary);
        renderSummaryCards(summary);

        setText('clusteringQualityTitle', quality.title || 'Оценка качества кластеризации');
        setText('clusteringQualitySubtitle', quality.subtitle || 'После расчета здесь появятся внутренние метрики качества и сравнение алгоритмов.');
        renderMetricCards('clusteringQualityMetrics', quality.metric_cards || [], 'После расчета здесь появятся внутренние метрики качества кластеризации.');
        renderMetricCards('clusteringQualityMethodology', quality.methodology_items || [], 'Методология сравнения появится после расчета.');
        renderQualityTable(quality.comparison_rows || []);
        renderNoticeList('clusteringQualityNotes', quality.dissertation_points || [], 'После расчета здесь появятся формулировки для раздела о качестве.');

        setText('clusterScatterTitle', charts.scatter ? charts.scatter.title : 'Кластеры территорий на двумерной проекции');
        setText('clusterDistributionTitle', charts.distribution ? charts.distribution.title : 'Размеры кластеров по числу территорий');
        setText('clusterDiagnosticsTitle', charts.diagnostics ? charts.diagnostics.title : 'Подсказка по числу кластеров');
        renderChart(charts.scatter, 'clusterScatterChart', 'clusterScatterChartFallback');
        renderChart(charts.distribution, 'clusterDistributionChart', 'clusterDistributionChartFallback');
        renderChart(charts.diagnostics, 'clusterDiagnosticsChart', 'clusterDiagnosticsChartFallback');

        renderDataTable('clusterCentroidTableShell', data.centroid_columns, data.centroid_rows, 'После расчета здесь появятся средние профили кластеров.');
        renderProfiles(data.cluster_profiles || []);
        renderNoticeList('clusterNotesList', data.notes || [], 'После расчета здесь появятся комментарии по качеству сегментации и смыслу полученных типов территорий.');
        renderDataTable('clusterRepresentativesTableShell', data.representative_columns, data.representative_rows, 'После расчета здесь появятся территории, ближайшие к центрам кластеров.');
        syncClusteringAsyncState(data);
    }
