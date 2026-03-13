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
        const tableName = payload && payload.table_name ? payload.table_name : 'Not selected';
        const queryText = payload && payload.query ? payload.query : 'Not set';
        const message = payload && payload.message ? payload.message : 'Совпадений не найдено.';

        setText('columnSearchTableTitle', tableName);
        setText('columnSearchHeroTable', tableName);
        setText('columnSearchHeroQuery', queryText);
        setText('columnSearchRowsInfo', 'Showing first 100 rows');

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

    async function fetchColumnSearch() {
        const tableSelect = byId('columnSearchTable');
        const queryInput = byId('columnSearchQuery');
        const button = byId('columnSearchButton');

        if (!tableSelect || !queryInput) {
            return;
        }

        const params = new URLSearchParams({
            table_name: tableSelect.value || '',
            query: queryInput.value || ''
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
            renderPreviewTable(payload);
            window.history.replaceState({}, '', '/column-search?' + params.toString());
        } catch (error) {
            console.error(error);
            renderPreviewTable({
                table_name: tableSelect.value || '',
                query: queryInput.value || '',
                preview_columns: [],
                preview_rows: [],
                message: 'Ошибка поиска колонок. Проверьте консоль приложения.'
            });
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const form = byId('columnSearchPageForm');
        const initial = window.__COLUMN_SEARCH_INITIAL__ || {};

        if (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                fetchColumnSearch();
            });
        }

        if (initial.query && initial.tableName) {
            fetchColumnSearch();
        }
    });
})();
