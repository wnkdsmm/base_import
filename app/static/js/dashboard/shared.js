// Shared helpers

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

    function getSelectedText(selectNode, fallback) {
        if (!selectNode || !selectNode.options.length) {
            return fallback;
        }
        const option = selectNode.options[selectNode.selectedIndex];
        return option ? option.text : fallback;
    }

    function renderFilterSummary(labels) {
        const summaryNode = byId('filterSummary');
        if (!summaryNode) {
            return;
        }

        if (labels) {
            summaryNode.textContent = 'Сейчас на панели: таблица ' + labels.table + ' | год ' + labels.year + ' | разрез ' + labels.group;
            return;
        }

        summaryNode.textContent = 'Сейчас на панели: таблица ' + getSelectedText(byId('tableFilter'), 'Все таблицы') +
            ' | год ' + getSelectedText(byId('yearFilter'), 'Все годы') +
            ' | разрез ' + getSelectedText(byId('groupColumnFilter'), 'Категория риска');
    }

    function setText(id, value) {
        const node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
    }

    function setHref(id, href) {
        const node = byId(id);
        if (node && href) {
            node.setAttribute('href', href);
        }
    }

    function setSelectOptions(selectId, options, selectedValue, emptyLabel) {
        const selectNode = byId(selectId);
        if (!selectNode) {
            return;
        }

        const selectedValues = Array.isArray(selectedValue)
            ? new Set(selectedValue.map(function (value) { return String(value); }))
            : new Set([String(selectedValue == null ? '' : selectedValue)]);
        const safeOptions = Array.isArray(options) && options.length ? options : [{ value: '', label: emptyLabel }];
        let currentGroup = '';
        let html = '';

        safeOptions.forEach(function (option) {
            const optionGroup = option.group || '';
            if (optionGroup !== currentGroup) {
                if (currentGroup) {
                    html += '</optgroup>';
                }
                if (optionGroup) {
                    html += '<optgroup label="' + escapeHtml(optionGroup) + '">';
                }
                currentGroup = optionGroup;
            }

            const selected = selectedValues.has(String(option.value)) ? ' selected' : '';
            html += '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        });

        if (currentGroup) {
            html += '</optgroup>';
        }

        selectNode.innerHTML = html;
    }
