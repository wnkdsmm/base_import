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

    function setStatus(message, tone) {
        var node = byId('tableActionStatus');
        if (!node) {
            return;
        }

        if (!message) {
            node.textContent = '';
            node.dataset.tone = '';
            node.classList.add('is-hidden');
            return;
        }

        node.textContent = message;
        node.dataset.tone = tone || 'info';
        node.classList.remove('is-hidden');
    }

    function setButtonBusy(button, isBusy, busyLabel) {
        if (!button) {
            return;
        }

        if (!button.dataset.defaultLabel) {
            button.dataset.defaultLabel = button.textContent || '';
        }

        if (isBusy) {
            button.dataset.wasDisabled = button.disabled ? 'true' : 'false';
            button.disabled = true;
        } else {
            button.disabled = button.dataset.wasDisabled === 'true';
            delete button.dataset.wasDisabled;
        }

        button.classList.toggle('is-loading', !!isBusy);
        button.textContent = isBusy ? (busyLabel || button.dataset.defaultLabel) : button.dataset.defaultLabel;
    }

    function updateCount(id, value) {
        var node = byId(id);
        if (node) {
            node.textContent = String(value);
        }
    }

    function updateAvailability(count) {
        var badge = byId('tableAvailabilityBadge');
        if (!badge) {
            return;
        }

        if (count > 0) {
            badge.textContent = 'База доступна';
            badge.classList.add('status-badge-live');
            return;
        }

        badge.textContent = 'Таблиц нет';
        badge.classList.remove('status-badge-live');
    }

    function buildTableCard(tableName) {
        var safeTable = String(tableName || '');
        var href = '/tables/' + encodeURIComponent(safeTable);

        return '<li class="table-link-item" data-table-row data-table-name="' + escapeHtml(safeTable) + '">' +
            '<article class="table-link-card table-link-record">' +
                '<div class="table-link-record-head">' +
                    '<label class="table-selection-check">' +
                        '<input class="table-selection-checkbox" type="checkbox" value="' + escapeHtml(safeTable) + '" aria-label="Выбрать таблицу ' + escapeHtml(safeTable) + '">' +
                    '</label>' +
                    '<a class="table-link-title" href="' + href + '">' + escapeHtml(safeTable) + '</a>' +
                '</div>' +
                '<div class="table-link-actions">' +
                    '<a class="table-link-inline-action" href="' + href + '">Открыть</a>' +
                '</div>' +
            '</article>' +
        '</li>';
    }

    function renderTableList(tableNames) {
        var list = byId('tableList');
        var emptyState = byId('tableListEmpty');
        if (!list || !emptyState) {
            return;
        }

        if (!Array.isArray(tableNames) || !tableNames.length) {
            list.innerHTML = '';
            list.classList.add('is-hidden');
            emptyState.classList.remove('is-hidden');
            return;
        }

        list.innerHTML = tableNames.map(buildTableCard).join('');
        list.classList.remove('is-hidden');
        emptyState.classList.add('is-hidden');
    }

    function getSelectionInputs() {
        return Array.prototype.slice.call(document.querySelectorAll('.table-selection-checkbox'));
    }

    function getSelectedTableNames() {
        return getSelectionInputs()
            .filter(function (input) { return input.checked; })
            .map(function (input) { return String(input.value || '').trim(); })
            .filter(function (tableName) { return !!tableName; });
    }

    function refreshSelectionState() {
        var inputs = getSelectionInputs();
        var selectedNames = getSelectedTableNames();
        var hasTables = inputs.length > 0;
        var selectedCount = selectedNames.length;

        var meta = byId('tablesSelectionMeta');
        if (meta) {
            meta.textContent = hasTables ? ('Выбрано: ' + selectedCount) : 'Таблиц нет';
        }

        var selectAllButton = byId('selectAllTablesButton');
        if (selectAllButton) {
            selectAllButton.disabled = !hasTables;
        }

        var clearButton = byId('clearSelectedTablesButton');
        if (clearButton) {
            clearButton.disabled = !selectedCount;
        }

        var deleteButton = byId('deleteSelectedTablesButton');
        if (deleteButton) {
            deleteButton.disabled = !selectedCount;
            deleteButton.dataset.defaultLabel = selectedCount > 0
                ? ('Удалить выбранные (' + selectedCount + ')')
                : 'Удалить выбранные';
            if (!deleteButton.classList.contains('is-loading')) {
                deleteButton.textContent = deleteButton.dataset.defaultLabel;
            }
        }
    }

    function applyTableState(tableNames) {
        var items = Array.isArray(tableNames) ? tableNames : [];
        renderTableList(items);
        updateCount('heroTablesCount', items.length);
        updateCount('sidebarTablesCount', items.length);
        updateAvailability(items.length);
        refreshSelectionState();
    }

    function selectAllTables(isChecked) {
        getSelectionInputs().forEach(function (input) {
            input.checked = !!isChecked;
        });
        refreshSelectionState();
    }

    function confirmDelete(tableNames) {
        var count = tableNames.length;
        var preview = tableNames.slice(0, 5).join(', ');
        var suffix = count > 5 ? '\n\nПервые выбранные: ' + preview + '...' : '\n\nВыбраны: ' + preview;
        return window.confirm('Удалить выбранные таблицы из базы данных?\n\nКоличество: ' + count + suffix + '\n\nЭто действие необратимо.');
    }

    async function requestBulkDelete(button) {
        var selectedNames = getSelectedTableNames();
        if (!selectedNames.length) {
            setStatus('Сначала отметьте таблицы галочками.', 'error');
            refreshSelectionState();
            return;
        }

        if (!confirmDelete(selectedNames)) {
            return;
        }

        setStatus('Удаляем выбранные таблицы...', 'info');
        setButtonBusy(button, true, 'Удаление...');

        try {
            var response = await fetch('/api/tables/delete', {
                method: 'POST',
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    table_names: selectedNames
                })
            });

            var payload = await response.json();
            if (!response.ok || !payload || payload.ok !== true) {
                throw new Error(payload && payload.message ? payload.message : 'Не удалось удалить таблицы.');
            }

            var remainingTables = Array.isArray(payload.remaining_tables) ? payload.remaining_tables : [];
            applyTableState(remainingTables);
            setStatus(payload.message || 'Выбранные таблицы удалены из базы данных.', 'success');
        } catch (error) {
            console.error(error);
            setStatus(error && error.message ? error.message : 'Не удалось удалить таблицы.', 'error');
        } finally {
            setButtonBusy(button, false);
            refreshSelectionState();
        }
    }

    function bindSelectionActions() {
        var selectAllButton = byId('selectAllTablesButton');
        if (selectAllButton) {
            selectAllButton.addEventListener('click', function () {
                selectAllTables(true);
            });
        }

        var clearButton = byId('clearSelectedTablesButton');
        if (clearButton) {
            clearButton.addEventListener('click', function () {
                selectAllTables(false);
            });
        }

        var deleteButton = byId('deleteSelectedTablesButton');
        if (deleteButton) {
            deleteButton.addEventListener('click', function () {
                requestBulkDelete(deleteButton);
            });
        }

        document.addEventListener('change', function (event) {
            var target = event.target;
            if (!(target instanceof Element) || !target.classList.contains('table-selection-checkbox')) {
                return;
            }

            refreshSelectionState();
        });
    }

    document.addEventListener('DOMContentLoaded', function () {
        window.fireTables = {
            afterImport: function () {
                setStatus('Импорт завершён. Обновляем список таблиц...', 'success');
                window.setTimeout(function () {
                    window.location.reload();
                }, 900);
            }
        };

        bindSelectionActions();
        refreshSelectionState();
    });
})();
