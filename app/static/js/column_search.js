(function () {
    const shared = window.FireUi;
    const byId = shared.byId;
    const escapeHtml = shared.escapeHtml;
    const fetchJson = shared.fetchJson;
    const setText = shared.setText;

    const state = {
        payload: null,
        selectedColumns: new Set(),
        selectedGroups: new Set(),
        excludedColumns: new Set(),
        lastTableName: '',
        previewRequestId: 0
    };

    function enableDragScroll(container) {
        if (!container || container.dataset.dragReady === '1') {
            return;
        }

        let isDragging = false;
        let startX = 0;
        let startY = 0;
        let scrollLeft = 0;
        let scrollTop = 0;

        container.dataset.dragReady = '1';

        container.addEventListener('mousedown', function (event) {
            isDragging = true;
            startX = event.pageX;
            startY = event.pageY;
            scrollLeft = container.scrollLeft;
            scrollTop = container.scrollTop;
            container.classList.add('is-dragging');
        });

        container.addEventListener('mousemove', function (event) {
            if (!isDragging) {
                return;
            }
            event.preventDefault();
            const deltaX = event.pageX - startX;
            const deltaY = event.pageY - startY;
            container.scrollLeft = scrollLeft - deltaX;
            container.scrollTop = scrollTop - deltaY;
        });

        ['mouseup', 'mouseleave'].forEach(function (eventName) {
            container.addEventListener(eventName, function () {
                isDragging = false;
                container.classList.remove('is-dragging');
            });
        });
    }

    function renderPreviewTable(payload) {
        const previewNode = byId('columnSearchPreview');
        if (!previewNode) {
            return;
        }

        const columns = payload && Array.isArray(payload.preview_columns) ? payload.preview_columns : [];
        const rows = payload && Array.isArray(payload.preview_rows) ? payload.preview_rows : [];
        const tableName = payload && payload.table_name ? payload.table_name : (payload && payload.source_table ? payload.source_table : 'Не выбрана');
        const message = payload && payload.message ? payload.message : 'Совпадений не найдено.';

        setText('columnSearchTableTitle', tableName);
        setText('columnSearchRowsInfo', rows.length ? ('Показаны первые ' + rows.length + ' строк') : 'Нет строк для предпросмотра');

        if (!columns.length) {
            previewNode.innerHTML = '<div class="mini-empty">' + escapeHtml(message) + '</div>';
            return;
        }

        const useCards = columns.length <= 5;
        const tableClassName = useCards
            ? 'preview-table table-stack-mobile'
            : 'preview-table table-sticky-first';

        const header = '<tr>' + columns.map(function (column) {
            return '<th>' + escapeHtml(column) + '</th>';
        }).join('') + '</tr>';

        const body = rows.length
            ? rows.map(function (row) {
                const cells = Array.isArray(row) ? row : [row];
                return '<tr>' + cells.map(function (cell, index) {
                    const label = useCards ? ' data-label="' + escapeHtml(columns[index] || '') + '"' : '';
                    return '<td' + label + '>' + escapeHtml(cell == null ? '' : String(cell)) + '</td>';
                }).join('') + '</tr>';
            }).join('')
            : '<tr><td colspan="' + columns.length + '">Нет строк для предпросмотра.</td></tr>';

        previewNode.innerHTML = '<div class="table-scroll"><table class="' + tableClassName + '"><thead>' + header + '</thead><tbody>' + body + '</tbody></table></div>';
        enableDragScroll(previewNode.querySelector('.table-scroll'));
    }

    function isColumnProvidedBySelectedGroup(columnName) {
        const groups = state.payload && Array.isArray(state.payload.groups) ? state.payload.groups : [];
        return groups.some(function (group) {
            return state.selectedGroups.has(group.id) && Array.isArray(group.columns) && group.columns.includes(columnName);
        });
    }

    function getSelectedColumnsUnion() {
        const selected = new Set();

        state.selectedColumns.forEach(function (column) {
            if (!state.excludedColumns.has(column)) {
                selected.add(column);
            }
        });

        const groups = state.payload && Array.isArray(state.payload.groups) ? state.payload.groups : [];
        groups.forEach(function (group) {
            if (state.selectedGroups.has(group.id) && Array.isArray(group.columns)) {
                group.columns.forEach(function (column) {
                    if (!state.excludedColumns.has(column)) {
                        selected.add(column);
                    }
                });
            }
        });

        return selected;
    }

    function isColumnSelected(columnName) {
        return getSelectedColumnsUnion().has(columnName);
    }

    function getGroupLabelsForColumn(columnName) {
        const groups = state.payload && Array.isArray(state.payload.groups) ? state.payload.groups : [];
        return groups
            .filter(function (group) {
                return Array.isArray(group.columns) && group.columns.includes(columnName);
            })
            .map(function (group) {
                return group.label;
            });
    }

    function buildVisibleMatches() {
        const payloadColumns = state.payload && Array.isArray(state.payload.columns) ? state.payload.columns : [];
        const visible = [];
        const byName = new Map();

        payloadColumns.forEach(function (item) {
            byName.set(item.name, item);
            visible.push(item);
        });

        const groups = state.payload && Array.isArray(state.payload.groups) ? state.payload.groups : [];
        groups.forEach(function (group) {
            if (!state.selectedGroups.has(group.id) || !Array.isArray(group.columns)) {
                return;
            }

            group.columns.forEach(function (columnName) {
                if (byName.has(columnName)) {
                    return;
                }

                const groupLabels = getGroupLabelsForColumn(columnName);
                const syntheticItem = {
                    name: columnName,
                    matched_terms: [],
                    score: '',
                    important_label: '',
                    group_ids: [],
                    group_labels: groupLabels,
                    match_mode: 'group'
                };
                byName.set(columnName, syntheticItem);
                visible.push(syntheticItem);
            });
        });

        return visible;
    }

    function updateSelectionSummary() {
        const summaryNode = byId('columnSearchSelectionSummary');
        if (!summaryNode) {
            return;
        }

        const selectedUnion = getSelectedColumnsUnion();
        const groupCount = state.selectedGroups.size;
        summaryNode.textContent = 'Выбрано колонок: ' + selectedUnion.size + ' | Выбрано групп: ' + groupCount;
    }

    async function refreshPreviewForSelection() {
        const tableSelect = byId('columnSearchTable');
        const previewNode = byId('columnSearchPreview');
        if (!tableSelect) {
            return;
        }

        const selectedColumns = Array.from(getSelectedColumnsUnion());
        const requestId = ++state.previewRequestId;

        if (!selectedColumns.length) {
            renderPreviewTable({
                table_name: tableSelect.value || '',
                preview_columns: [],
                preview_rows: [],
                message: 'Выберите колонки или тематические группы для предпросмотра.'
            });
            return;
        }

        if (previewNode) {
            previewNode.innerHTML = '<div class="mini-empty">Загружается предпросмотр...</div>';
        }

        try {
            const result = await fetchJson('/api/column-search/preview', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({
                    table_name: tableSelect.value || '',
                    selected_columns: selectedColumns
                })
            }, 'Не удалось загрузить предпросмотр.');
            const payload = result.payload;
            if (requestId !== state.previewRequestId) {
                return;
            }
            renderPreviewTable(payload);
        } catch (error) {
            if (requestId !== state.previewRequestId) {
                return;
            }
            renderPreviewTable({
                table_name: tableSelect.value || '',
                preview_columns: [],
                preview_rows: [],
                message: 'Не удалось загрузить предпросмотр.'
            });
        }
    }

    function renderGroups(groups) {
        const node = byId('columnSearchGroups');
        if (!node) {
            return;
        }

        if (!groups || !groups.length) {
            node.innerHTML = '<div class="mini-empty">Для этой таблицы группы не найдены.</div>';
            updateSelectionSummary();
            return;
        }

        node.innerHTML = groups.map(function (group) {
            const checked = state.selectedGroups.has(group.id) ? 'checked' : '';
            const columnsPreview = Array.isArray(group.columns) && group.columns.length
                ? group.columns.slice(0, 4).map(escapeHtml).join(', ')
                : 'Совпадений в этой группе пока нет';
            const moreLabel = Array.isArray(group.columns) && group.columns.length > 4
                ? ' и еще ' + (group.columns.length - 4)
                : '';

            return '' +
                '<label class="column-group-card">' +
                    '<input class="column-group-checkbox" type="checkbox" data-group-id="' + escapeHtml(group.id) + '" ' + checked + '>' +
                    '<span class="column-group-body">' +
                        '<span class="column-group-title-row">' +
                            '<strong>' + escapeHtml(group.label) + '</strong>' +
                            '<span class="column-group-count">' + escapeHtml(group.count) + '</span>' +
                        '</span>' +
                        '<span class="column-group-description">' + escapeHtml(group.description) + '</span>' +
                        '<span class="column-group-columns">' + columnsPreview + escapeHtml(moreLabel) + '</span>' +
                    '</span>' +
                '</label>';
        }).join('');

        Array.from(node.querySelectorAll('.column-group-checkbox')).forEach(function (checkbox) {
            checkbox.addEventListener('change', function (event) {
                const groupId = event.target.getAttribute('data-group-id');
                if (!groupId) {
                    return;
                }
                if (event.target.checked) {
                    state.selectedGroups.add(groupId);
                } else {
                    state.selectedGroups.delete(groupId);
                }
                renderMatches(buildVisibleMatches(), state.payload ? state.payload.message : '');
                updateSelectionSummary();
                refreshPreviewForSelection();
            });
        });

        updateSelectionSummary();
    }

    function renderMatches(columns, message) {
        const node = byId('columnSearchMatches');
        if (!node) {
            return;
        }

        if (!columns || !columns.length) {
            node.innerHTML = '<div class="mini-empty">' + escapeHtml(message || 'После поиска здесь появятся найденные колонки.') + '</div>';
            updateSelectionSummary();
            return;
        }

        const selectedUnion = getSelectedColumnsUnion();
        node.innerHTML = columns.map(function (item) {
            const isChecked = selectedUnion.has(item.name);
            const checked = isChecked ? 'checked' : '';
            const isExcluded = state.excludedColumns.has(item.name);
            const cardClass = 'column-match-card'
                + (isChecked ? ' selected' : '')
                + (isExcluded ? ' excluded' : '');
            const matchedTerms = Array.isArray(item.matched_terms) && item.matched_terms.length
                ? item.matched_terms.map(function (term) {
                    return '<span class="column-tag">' + escapeHtml(term) + '</span>';
                }).join('')
                : '';
            const groups = Array.isArray(item.group_labels) && item.group_labels.length
                ? item.group_labels.map(function (label) {
                    return '<span class="column-tag column-tag-muted">' + escapeHtml(label) + '</span>';
                }).join('')
                : '';
            const label = item.important_label ? '<span class="column-important-label">Важно: ' + escapeHtml(item.important_label) + '</span>' : '';
            const scoreLabel = item.score === '' || item.score == null ? '—' : item.score;
            const matchModeLabel = item.match_mode === 'group' ? 'group' : 'query';

            return '' +
                '<label class="' + cardClass + '">' +
                    '<input class="column-match-checkbox" type="checkbox" data-column-name="' + escapeHtml(item.name) + '" ' + checked + '>' +
                    '<span class="column-match-body">' +
                        '<strong class="column-match-name">' + escapeHtml(item.name) + '</strong>' +
                        '<span class="column-match-meta">Совпадение: ' + escapeHtml(matchModeLabel) + ' | Score: ' + escapeHtml(scoreLabel) + '</span>' +
                        label +
                        '<span class="column-tag-row">' + matchedTerms + groups + '</span>' +
                    '</span>' +
                '</label>';
        }).join('');

        Array.from(node.querySelectorAll('.column-match-checkbox')).forEach(function (checkbox) {
            checkbox.addEventListener('change', function (event) {
                const name = event.target.getAttribute('data-column-name');
                if (!name) {
                    return;
                }
                const card = event.target.closest('.column-match-card');

                if (event.target.checked) {
                    state.selectedColumns.add(name);
                    state.excludedColumns.delete(name);
                } else {
                    state.selectedColumns.delete(name);
                    if (isColumnProvidedBySelectedGroup(name)) {
                        state.excludedColumns.add(name);
                    } else {
                        state.excludedColumns.delete(name);
                    }
                }

                const isGroupProvided = isColumnProvidedBySelectedGroup(name);
                if (card) {
                    card.classList.toggle('selected', event.target.checked);
                    card.classList.toggle('excluded', !event.target.checked && isGroupProvided);
                }
                updateSelectionSummary();
                refreshPreviewForSelection();
            });
        });

        updateSelectionSummary();
    }

    function setStatus(message, isError) {
        const node = byId('columnSearchStatus');
        if (!node) {
            return;
        }
        node.textContent = message || '';
        node.classList.toggle('is-error', Boolean(isError));
    }

    async function fetchColumnSearch() {
        const tableSelect = byId('columnSearchTable');
        const queryInput = byId('columnSearchQuery');
        const button = byId('columnSearchButton');

        if (!tableSelect || !queryInput) {
            return;
        }

        const tableName = tableSelect.value || '';
        const queryText = queryInput.value || '';
        const params = new URLSearchParams({
            table_name: tableName,
            query: queryText
        });

        if (button) {
            button.disabled = true;
        }

        try {
            const result = await fetchJson('/api/column-search?' + params.toString(), {
                headers: { Accept: 'application/json' }
            }, 'Не удалось выполнить поиск колонок.');
            const payload = result.payload;
            const tableChanged = state.lastTableName !== payload.table_name;
            state.payload = payload;
            state.lastTableName = payload.table_name || '';

            if (tableChanged) {
                state.selectedGroups = new Set();
            }
            state.selectedColumns = new Set((payload.columns || []).map(function (item) {
                return item.name;
            }));
            state.excludedColumns = new Set();

            renderGroups(payload.groups || []);
            renderMatches(buildVisibleMatches(), payload.message || 'Совпадения не найдены.');
            renderPreviewTable(payload);
            setStatus(payload.message || '', false);
            window.history.replaceState({}, '', '/column-search?' + params.toString());
        } catch (error) {
            state.payload = null;
            state.selectedColumns = new Set();
            state.selectedGroups = new Set();
            state.excludedColumns = new Set();
            renderGroups([]);
            renderMatches([], 'Ошибка поиска колонок. Проверьте консоль приложения.');
            renderPreviewTable({
                table_name: tableName,
                preview_columns: [],
                preview_rows: [],
                message: 'Ошибка поиска колонок. Проверьте консоль приложения.'
            });
            setStatus('Ошибка поиска колонок. Проверьте консоль приложения.', true);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    async function createModifyTable() {
        const tableSelect = byId('columnSearchTable');
        const queryInput = byId('columnSearchQuery');
        const createButton = byId('columnSearchCreateTable');

        if (!tableSelect || !state.payload) {
            setStatus('Сначала выберите таблицу и загрузите колонки.', true);
            return;
        }

        const selectedColumns = Array.from(getSelectedColumnsUnion());
        const selectedGroups = Array.from(state.selectedGroups);
        const finalSelectedCount = selectedColumns.length;

        if (!finalSelectedCount) {
            setStatus('Нужно выбрать хотя бы одну колонку или одну тематическую группу.', true);
            return;
        }

        if (createButton) {
            createButton.disabled = true;
        }

        try {
            const result = await fetchJson('/api/column-search/create-modify-table', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({
                    table_name: tableSelect.value || '',
                    query: queryInput ? queryInput.value || '' : '',
                    selected_columns: selectedColumns,
                    selected_groups: selectedGroups
                })
            }, 'Не удалось создать таблицу с префиксом modify_.');
            const payload = result.payload;
            if (payload.status === 'error') {
                throw new Error(payload.message || 'Не удалось создать таблицу с префиксом modify_.');
            }

            renderPreviewTable(payload);
            setStatus(
                'Создана таблица ' + payload.table_name + '. Колонок: ' + payload.columns_count + '. ' + (payload.message || ''),
                false
            );
        } catch (error) {
            setStatus(error.message || 'Не удалось создать таблицу с префиксом modify_.', true);
        } finally {
            if (createButton) {
                createButton.disabled = false;
            }
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const form = byId('columnSearchPageForm');
        const selectAllButton = byId('columnSearchSelectAll');
        const clearButton = byId('columnSearchClearSelection');
        const createButton = byId('columnSearchCreateTable');
        const initial = window.__COLUMN_SEARCH_INITIAL__ || {};

        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchColumnSearch();
            });
        }

        if (selectAllButton) {
            selectAllButton.addEventListener('click', function () {
                const visibleMatches = buildVisibleMatches();
                if (!visibleMatches.length) {
                    return;
                }
                visibleMatches.forEach(function (item) {
                    state.selectedColumns.add(item.name);
                    state.excludedColumns.delete(item.name);
                });
                renderMatches(visibleMatches, state.payload ? state.payload.message : '');
                refreshPreviewForSelection();
            });
        }

        if (clearButton) {
            clearButton.addEventListener('click', function () {
                const visibleMatches = buildVisibleMatches();
                state.selectedColumns = new Set();
                state.excludedColumns = new Set(visibleMatches.map(function (item) {
                    return item.name;
                }));
                renderGroups(state.payload ? state.payload.groups : []);
                renderMatches(buildVisibleMatches(), state.payload ? state.payload.message : '');
                refreshPreviewForSelection();
            });
        }

        if (createButton) {
            createButton.addEventListener('click', function () {
                createModifyTable();
            });
        }

        if (initial.tableName) {
            fetchColumnSearch();
        }
    });
})();

