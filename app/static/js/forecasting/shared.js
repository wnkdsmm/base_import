// Shared helpers and state

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

    function applyProgressBars(root) {
        var scope = root && typeof root.querySelectorAll === 'function' ? root : document;
        Array.prototype.forEach.call(scope.querySelectorAll('[data-bar-width]'), function (node) {
            node.style.setProperty('--bar-width', normalizePercent(node.getAttribute('data-bar-width'), '0%'));
        });
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

    function setSelectOptions(id, options, selectedValue, emptyLabel) {
        var selectNode = byId(id);
        if (!selectNode) {
            return;
        }

        var safeOptions = Array.isArray(options) && options.length ? options : [{ value: 'all', label: emptyLabel }];
        selectNode.innerHTML = safeOptions.map(function (option) {
            var selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        }).join('');
    }


    var currentForecastData = window.__FIRE_FORECAST_INITIAL__ || null;
    var forecastRequestToken = 0;
    var forecastStepTimers = [];
    var decisionSupportJobPollTimer = null;

    function applyToneClass(node, tone) {
        if (!node) {
            return;
        }

        node.className = node.className.replace(/\btone-[a-z]+\b/g, '').replace(/\s+/g, ' ').trim();
        if (tone) {
            node.className += (node.className ? ' ' : '') + 'tone-' + tone;
        }
    }

    function normalizeTone(tone) {
        if (tone === 'high') {
            return 'fire';
        }
        if (tone === 'medium') {
            return 'sand';
        }
        if (tone === 'low') {
            return 'sky';
        }
        return tone || 'sky';
    }
