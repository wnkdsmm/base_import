(function (global) {
    var uiHelpers = global.FireUiHelpers || {};
    var plotlyHelpers = global.FirePlotlyHelpers || {};
    var apiClient = global.FireApiClient || {};

    function byId(id) {
        return typeof uiHelpers.byId === 'function' ? uiHelpers.byId(id) : document.getElementById(id);
    }

    function setText(nodeOrId, value) {
        if (typeof uiHelpers.setText === 'function') {
            uiHelpers.setText(nodeOrId, value);
            return;
        }
        var node = typeof nodeOrId === 'string' ? byId(nodeOrId) : nodeOrId;
        if (node) {
            node.textContent = value == null ? '' : value;
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

    function getErrorMessage(error, fallback) {
        var message = error && typeof error.message === 'string' ? error.message.trim() : '';
        return message || fallback;
    }

    function revealPageContent() {
        if (document && document.body) {
            document.body.removeAttribute('data-page-loading');
        }
    }

    global.FireUi = {
        applyToneClass: uiHelpers.applyToneClass,
        byId: byId,
        createJobId: uiHelpers.createJobId,
        createApiError: apiClient.createApiError,
        createSingleTimer: createSingleTimer,
        createTimerGroup: createTimerGroup,
        escapeHtml: uiHelpers.escapeHtml,
        apiCall: apiClient.apiCall,
        fetchJson: apiClient.apiCall,
        getApiErrorMessage: apiClient.getApiErrorMessage,
        getErrorMessage: getErrorMessage,
        normalizeCssColor: plotlyHelpers.normalizeCssColor,
        normalizePercent: plotlyHelpers.normalizePercent,
        renderListItems: uiHelpers.renderListItems,
        renderMetricCards: uiHelpers.renderMetricCards,
        renderPlotlyFigure: plotlyHelpers.renderPlotlyFigure,
        renderPlotlyInContainer: plotlyHelpers.renderPlotlyInContainer,
        runProgressSequence: runProgressSequence,
        revealPageContent: revealPageContent,
        setHref: uiHelpers.setHref,
        setSectionHidden: uiHelpers.setSectionHidden,
        setSelectOptions: uiHelpers.setSelectOptions,
        setStepProgress: setStepProgress,
        setText: uiHelpers.setText,
        setValue: uiHelpers.setValue
    };
}(window));
