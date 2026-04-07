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

    function createTimerGroup() {
        var timers = [];
        return {
            clear: function () {
                while (timers.length) {
                    clearTimeout(timers.pop());
                }
            },
            set: function (callback, delay) {
                var timer = setTimeout(callback, delay);
                timers.push(timer);
                return timer;
            },
            push: function (timer) {
                timers.push(timer);
                return timer;
            }
        };
    }

    function createSingleTimer() {
        var timer = null;
        return {
            clear: function () {
                if (timer) {
                    clearTimeout(timer);
                    timer = null;
                }
            },
            set: function (callback, delay) {
                this.clear();
                timer = setTimeout(callback, delay);
                return timer;
            }
        };
    }

    function runProgressSequence(timerGroup, updateProgress, entries) {
        var safeEntries = Array.isArray(entries) ? entries : [];
        if (timerGroup && typeof timerGroup.clear === 'function') {
            timerGroup.clear();
        }
        safeEntries.forEach(function (entry) {
            var safeEntry = entry || {};
            var run = function () {
                updateProgress(safeEntry.activeIndex, safeEntry.options || {});
            };
            if (safeEntry.delay && safeEntry.delay > 0 && timerGroup && typeof timerGroup.set === 'function') {
                timerGroup.set(run, safeEntry.delay);
                return;
            }
            run();
        });
    }

    function setStepProgress(config) {
        var settings = config || {};
        var activeIndex = settings.activeIndex;
        var stepsNode = byId(settings.stepsId);
        var lead = settings.lead || '';
        var message = settings.message || '';
        var isFinished = Boolean(settings.isFinished);
        var isError = Boolean(settings.isError);
        var stepSelector = settings.stepSelector || '.analysis-step';

        setText(settings.leadId, lead);
        setText(settings.messageId, message);

        if (!stepsNode) {
            return;
        }

        Array.prototype.forEach.call(stepsNode.querySelectorAll(stepSelector), function (node, index) {
            node.classList.remove('is-active', 'is-done', 'is-error');
            if (isError && index === activeIndex) {
                node.classList.add('is-error');
                return;
            }
            if (isFinished) {
                if (index <= activeIndex) {
                    node.classList.add('is-done');
                }
                return;
            }
            if (index < activeIndex) {
                node.classList.add('is-done');
                return;
            }
            if (index === activeIndex) {
                node.classList.add('is-active');
            }
        });
    }

    function getApiErrorMessage(payload, fallback) {
        var normalizedFallback = fallback || 'Request failed.';
        if (!payload || typeof payload !== 'object') {
            return normalizedFallback;
        }

        if (payload.error && typeof payload.error === 'object') {
            var apiMessage = String(payload.error.message || payload.error.detail || payload.error.code || '').trim();
            if (apiMessage) {
                return apiMessage;
            }
        }

        var legacyMessage = String(payload.error_message || payload.detail || payload.message || '').trim();
        return legacyMessage || normalizedFallback;
    }

    function createApiError(response, payload, fallback) {
        var error = new Error(getApiErrorMessage(payload, fallback));
        error.status = response && typeof response.status === 'number' ? response.status : 0;
        error.payload = payload || null;
        return error;
    }

    function getErrorMessage(error, fallback) {
        var message = error && typeof error.message === 'string' ? error.message.trim() : '';
        return message || fallback;
    }

    async function fetchJson(url, options, fallback, invalidJsonFallback) {
        var response = await fetch(url, options || {});
        var payload;
        try {
            payload = await response.json();
        } catch (error) {
            if (!response.ok) {
                throw createApiError(response, null, invalidJsonFallback || fallback);
            }
            throw error;
        }
        if (!response.ok) {
            throw createApiError(response, payload, fallback);
        }
        if (payload && payload.ok === false) {
            throw createApiError(response, payload, fallback);
        }
        return {
            payload: payload,
            response: response
        };
    }

    global.FireUi = {
        applyToneClass: applyToneClass,
        byId: byId,
        createApiError: createApiError,
        createSingleTimer: createSingleTimer,
        createTimerGroup: createTimerGroup,
        escapeHtml: escapeHtml,
        fetchJson: fetchJson,
        getApiErrorMessage: getApiErrorMessage,
        getErrorMessage: getErrorMessage,
        normalizePercent: normalizePercent,
        renderPlotlyFigure: renderPlotlyFigure,
        runProgressSequence: runProgressSequence,
        setHref: setHref,
        setSelectOptions: setSelectOptions,
        setStepProgress: setStepProgress,
        setText: setText,
        setValue: setValue
    };
}(window));
