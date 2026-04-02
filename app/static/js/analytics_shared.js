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

    function setText(id, value) {
        var node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
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

    function normalizePercent(value, fallback) {
        var normalizedFallback = fallback || '0%';
        var rawValue = String(value == null ? '' : value).trim();
        var match = rawValue.match(/^(-?\d+(?:\.\d+)?)%?$/);
        if (!match) {
            return normalizedFallback;
        }

        var numericValue = Math.max(0, Math.min(100, Number(match[1])));
        return numericValue + '%';
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

    function renderPlotlyFigure(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return false;
        }

        var figure = chart && chart.plotly;
        if (!window.Plotly || !figure || !Array.isArray(figure.data) || !figure.data.length) {
            chartNode.innerHTML = '';
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            return false;
        }

        fallbackNode.textContent = '';
        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
        return true;
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

    global.FireUi = {
        applyToneClass: applyToneClass,
        byId: byId,
        escapeHtml: escapeHtml,
        normalizePercent: normalizePercent,
        renderPlotlyFigure: renderPlotlyFigure,
        setHref: setHref,
        setSelectOptions: setSelectOptions,
        setText: setText,
        setValue: setValue
    };
}(window));
