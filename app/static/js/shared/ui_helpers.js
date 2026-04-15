(function (global) {
    var escapeHtml = global.FireUi && typeof global.FireUi.escapeHtml === 'function'
        ? global.FireUi.escapeHtml
        : function (value) { return String(value == null ? '' : value); };

    function byId(id) {
        return document.getElementById(id);
    }

    function setHidden(nodeOrId, hidden) {
        var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
        if (!node) {
            return;
        }
        node.classList.toggle('is-hidden', !!hidden);
    }

    function setText(nodeOrId, value) {
        var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
        if (!node) {
            return;
        }
        node.textContent = value == null ? '' : value;
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

    global.FireUiHelpers = {
        renderList: renderList,
        setHidden: setHidden,
        setText: setText
    };
}(window));
