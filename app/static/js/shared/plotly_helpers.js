(function (global) {
    var uiHelpers = global.FireUiHelpers || {};
    var byId = typeof uiHelpers.byId === 'function'
        ? uiHelpers.byId
        : function (id) { return document.getElementById(id); };
    var escapeHtml = typeof uiHelpers.escapeHtml === 'function'
        ? uiHelpers.escapeHtml
        : function (value) { return String(value == null ? '' : value); };

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

    function normalizeCssColor(value, fallback) {
        var normalizedFallback = fallback || 'currentColor';
        var candidate = String(value == null ? '' : value).trim();
        if (!candidate) {
            return normalizedFallback;
        }

        var probe = document.createElement('span');
        probe.style.color = '';
        probe.style.color = candidate;
        return probe.style.color ? candidate : normalizedFallback;
    }

    function renderPlotlyFigure(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return false;
        }

        var figure = chart && chart.plotly;
        if (!global.Plotly || !figure || !Array.isArray(figure.data) || !figure.data.length) {
            chartNode.innerHTML = '';
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            return false;
        }

        fallbackNode.textContent = '';
        fallbackNode.classList.add('is-hidden');
        global.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
        return true;
    }

    function renderPlotlyInContainer(chart, containerId, options) {
        var container = byId(containerId);
        var settings = options || {};
        var emptyClass = settings.emptyClass || 'chart-empty';
        var defaultMessage = settings.emptyMessage || 'Interactive chart is unavailable.';

        if (!container) {
            return false;
        }

        var figure = chart && chart.plotly;
        if (!figure || !global.Plotly) {
            container.innerHTML = '<div class="' + escapeHtml(emptyClass) + '">' + escapeHtml(chart && chart.empty_message ? chart.empty_message : defaultMessage) + '</div>';
            return false;
        }

        var plotlyApi = global.Plotly;
        var renderChart = typeof plotlyApi.newPlot === 'function'
            ? plotlyApi.newPlot.bind(plotlyApi)
            : plotlyApi.react.bind(plotlyApi);
        var data = Array.isArray(figure.data) ? figure.data : [];
        var layout = figure.layout || {};
        var config = figure.config || { responsive: true };

        try {
            if (typeof plotlyApi.purge === 'function') {
                plotlyApi.purge(container);
            }
            container.innerHTML = '';
            var renderPromise = renderChart(container, data, layout, config);
            if (renderPromise && typeof renderPromise.catch === 'function') {
                renderPromise.catch(function () {
                    container.innerHTML = '<div class="' + escapeHtml(emptyClass) + '">' + escapeHtml(chart && chart.empty_message ? chart.empty_message : defaultMessage) + '</div>';
                });
            }
        } catch (error) {
            container.innerHTML = '<div class="' + escapeHtml(emptyClass) + '">' + escapeHtml(chart && chart.empty_message ? chart.empty_message : defaultMessage) + '</div>';
            return false;
        }

        return true;
    }

    global.FirePlotlyHelpers = {
        normalizeCssColor: normalizeCssColor,
        normalizePercent: normalizePercent,
        renderPlotlyFigure: renderPlotlyFigure,
        renderPlotlyInContainer: renderPlotlyInContainer
    };
}(window));
