(function () {
    const state = {
        payload: null,
        selectedColumns: new Set(),
        selectedGroups: new Set(),
        lastTableName: ''
    };

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
        const node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
    }

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
        const tableName = payload && payload.table_name ? payload.table_name : (payload && payload.source_table ? payload.source_table : 'Not selected');
        const message = payload && payload.message ? payload.message : 'Совпадений не найдено.';

        setText('columnSearchTableTitle', tableName);
        setText('columnSearchRowsInfo', rows.length ? ('Показаны первые ' + rows.length + ' строк') : 'Нет строк для предпросмотра');

        if (!columns.length) {
            previewNode.innerHTML = '<div class="mini-empty">' + escapeHtml(message) + '</div>';
            return;
        }

        const header = '<tr>' + columns.map(function (column) {
            return '<th>' + escapeHtml(column) + '</th>';
        }).join('') + '</tr>';

        const body = rows.length
            ? rows.map(function (row) {
                const cells = Array.isArray(row) ? row : [row];
                return '<tr>' + cells.map(function (cell) {
                    return '<td>' + escapeHtml(cell == null ? '' : String(cell)) + '</td>';
                }).join('') + '</tr>';
            }).join('')
            : '<tr><td colspan="' + columns.length + '">Нет строк для предпросмотра.</td></tr>';

        previewNode.innerHTML = '<div class="table-scroll"><table class="preview-table"><thead>' + header + '</thead><tbody>' + body + '</tbody></table></div>';
        enableDragScroll(previewNode.querySelector('.table-scroll'));
    }

    function getSelectedColumnsUnion() {
        const selected = new Set(state.selectedColumns);
        const groups = state.payload && Array.isArray(state.payload.groups) ? state.payload.groups : [];
        groups.forEach(function (group) {
            if (state.selectedGroups.has(group.id) && Array.isArray(group.columns)) {
                group.columns.forEach(function (column) {
                    selected.add(column);
                });
            }
        });
        return selected;
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
                updateSelectionSummary();
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

        node.innerHTML = columns.map(function (item) {
            const checked = state.selectedColumns.has(item.name) ? 'checked' : '';
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

            return '' +
                '<label class="column-match-card">' +
                    '<input class="column-match-checkbox" type="checkbox" data-column-name="' + escapeHtml(item.name) + '" ' + checked + '>' +
                    '<span class="column-match-body">' +
                        '<strong class="column-match-name">' + escapeHtml(item.name) + '</strong>' +
                        '<span class="column-match-meta">Совпадение: ' + escapeHtml(item.match_mode || 'query') + ' | Score: ' + escapeHtml(item.score == null ? '' : item.score) + '</span>' +
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
                if (event.target.checked) {
                    state.selectedColumns.add(name);
                } else {
                    state.selectedColumns.delete(name);
                }
                updateSelectionSummary();
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
            const response = await fetch('/api/column-search?' + params.toString(), {
                headers: { 'Accept': 'application/json' }
            });
            if (!response.ok) {
                throw new Error('Не удалось выполнить поиск колонок');
            }
            const payload = await response.json();
            const tableChanged = state.lastTableName !== payload.table_name;
            state.payload = payload;
            state.lastTableName = payload.table_name || '';
            state.selectedGroups = tableChanged ? new Set() : state.selectedGroups;
            state.selectedColumns = new Set(payload.columns.map(function (item) {
                return item.name;
            }));

            renderGroups(payload.groups || []);
            renderMatches(payload.columns || [], payload.message || 'Совпадения не найдены.');
            renderPreviewTable(payload);
            setStatus(payload.message || '', false);
            window.history.replaceState({}, '', '/column-search?' + params.toString());
        } catch (error) {
            console.error(error);
            state.payload = null;
            state.selectedColumns = new Set();
            state.selectedGroups = new Set();
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

        const selectedColumns = Array.from(state.selectedColumns);
        const selectedGroups = Array.from(state.selectedGroups);
        const finalSelectedCount = getSelectedColumnsUnion().size;

        if (!finalSelectedCount) {
            setStatus('Нужно выбрать хотя бы одну колонку или одну тематическую группу.', true);
            return;
        }

        if (createButton) {
            createButton.disabled = true;
        }

        try {
            const response = await fetch('/api/column-search/create-modify-table', {
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
            });
            const payload = await response.json();
            if (!response.ok || payload.status === 'error') {
                throw new Error(payload.message || 'Не удалось создать modify-таблицу');
            }

            renderPreviewTable(payload);
            setStatus(
                'Создана таблица ' + payload.table_name + '. Колонок: ' + payload.columns_count + '. ' + (payload.message || ''),
                false
            );
        } catch (error) {
            console.error(error);
            setStatus(error.message || 'Не удалось создать modify-таблицу.', true);
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
                if (!state.payload || !Array.isArray(state.payload.columns)) {
                    return;
                }
                state.selectedColumns = new Set(state.payload.columns.map(function (item) {
                    return item.name;
                }));
                renderMatches(state.payload.columns, state.payload.message);
            });
        }

        if (clearButton) {
            clearButton.addEventListener('click', function () {
                state.selectedColumns = new Set();
                state.selectedGroups = new Set();
                renderGroups(state.payload ? state.payload.groups : []);
                renderMatches(state.payload ? state.payload.columns : [], state.payload ? state.payload.message : '');
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

