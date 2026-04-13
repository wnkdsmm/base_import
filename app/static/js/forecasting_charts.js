(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules = window.ForecastingModules || {};
    var normalizePercent = shared.normalizePercent;
    var renderChart = shared.renderPlotlyFigure;

    function applyProgressBars(root) {
        var scope = root && typeof root.querySelectorAll === 'function' ? root : document;
        Array.prototype.forEach.call(scope.querySelectorAll('[data-bar-width]'), function (node) {
            node.style.setProperty('--bar-width', normalizePercent(node.getAttribute('data-bar-width'), '0%'));
        });
    }

    function renderForecastCharts(charts) {
        var safeCharts = charts || {};
        renderChart(safeCharts.daily, 'forecastDailyChart', 'forecastDailyChartFallback');
        renderChart(safeCharts.weekday, 'forecastWeekdayChart', 'forecastWeekdayChartFallback');
    }

    modules.createForecastingCharts = function createForecastingCharts() {
        return {
            applyProgressBars: applyProgressBars,
            renderForecastCharts: renderForecastCharts
        };
    };
})();
