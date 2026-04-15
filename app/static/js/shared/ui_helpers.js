(function (global) {
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

    function setHidden(nodeOrId, hidden) {
        var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
        if (!node) {
            return;
        }
        node.classList.toggle('is-hidden', !!hidden);
    }

    function setSectionHidden(id, isHidden) {
        setHidden(id, isHidden);
    }

    function setText(nodeOrId, value) {
        var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
        if (!node) {
            return;
        }
        node.textContent = value == null ? '' : value;
    }

    function setHref(id, href) {
        var node = byId(id);
        if (node && href) {
            node.setAttribute('href', href);
        }
    }

    function setValue(id, value) {
        var node = byId(id);
        if (node) {
            node.value = value == null ? '' : value;
        }
    }

    function setSelectOptions(id, options, selectedValue, emptyLabel, config) {
        var selectNode = byId(id);
        if (!selectNode) {
            return;
        }

        var settings = config || {};
        var selectedValues = Array.isArray(selectedValue)
            ? new Set(selectedValue.map(function (value) { return String(value); }))
            : new Set([String(selectedValue == null ? '' : selectedValue)]);
        var fallbackValue = settings.emptyValue != null
            ? settings.emptyValue
            : (Array.isArray(selectedValue)
                ? ''
                : (String(selectedValue == null ? '' : selectedValue) === 'all' ? 'all' : ''));
        var safeOptions = Array.isArray(options) && options.length
            ? options
            : [{ value: fallbackValue, label: emptyLabel }];
        var currentGroup = '';
        var html = '';

        safeOptions.forEach(function (option) {
            var safeOption = option || {};
            var optionGroup = settings.useGroups === false ? '' : String(safeOption.group || '');
            if (optionGroup !== currentGroup) {
                if (currentGroup) {
                    html += '</optgroup>';
                }
                if (optionGroup) {
                    html += '<optgroup label="' + escapeHtml(optionGroup) + '">';
                }
                currentGroup = optionGroup;
            }

            var selected = selectedValues.has(String(safeOption.value)) ? ' selected' : '';
            html += '<option value="' + escapeHtml(safeOption.value) + '"' + selected + '>' + escapeHtml(safeOption.label) + '</option>';
        });

        if (currentGroup) {
            html += '</optgroup>';
        }

        selectNode.innerHTML = html;
    }

    function renderList(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return false;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<li>' + escapeHtml(emptyMessage || '') + '</li>';
            return false;
        }

        container.innerHTML = items.map(function (item) {
            return '<li>' + escapeHtml(item) + '</li>';
        }).join('');
        return true;
    }

    function renderListItems(containerId, items, emptyMessage, options) {
        var settings = options || {};
        var safeItems = Array.isArray(items) ? items : [];

        if (settings.filterEmpty) {
            safeItems = safeItems.filter(function (item) {
                return String(item == null ? '' : item).trim().length > 0;
            });
        }
        if (settings.limit && settings.limit > 0) {
            safeItems = safeItems.slice(0, settings.limit);
        }

        return renderList(containerId, safeItems, emptyMessage);
    }

    function renderMetricCards(containerId, items, emptyMessage) {
        var container = byId(containerId);
        if (!container) {
            return;
        }

        if (!Array.isArray(items) || !items.length) {
            container.innerHTML = '<div class="mini-empty">' + escapeHtml(emptyMessage || '') + '</div>';
            return;
        }

        container.innerHTML = items.map(function (item) {
            return ''
                + '<article class="stat-card">'
                + '<span class="stat-label">' + escapeHtml(item && item.label ? item.label : '-') + '</span>'
                + '<strong class="stat-value">' + escapeHtml(item && item.value ? item.value : '-') + '</strong>'
                + '<span class="stat-foot">' + escapeHtml(item && item.meta ? item.meta : '') + '</span>'
                + '</article>';
        }).join('');
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

    global.FireUiHelpers = {
        applyToneClass: applyToneClass,
        byId: byId,
        escapeHtml: escapeHtml,
        renderList: renderList,
        renderListItems: renderListItems,
        renderMetricCards: renderMetricCards,
        setHidden: setHidden,
        setHref: setHref,
        setSectionHidden: setSectionHidden,
        setSelectOptions: setSelectOptions,
        setText: setText,
        setValue: setValue
    };
}(window));
