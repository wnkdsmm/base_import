(function () {
    var data = window.__FIRE_CLUSTERING_INITIAL__;
    if (!data) {
        return;
    }

    function byId(id) {
        return document.getElementById(id);
    }

    function renderChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var figure = chart && chart.plotly;
        if (!window.Plotly || !figure || !Array.isArray(figure.data) || !figure.data.length) {
            chartNode.innerHTML = '';
            fallbackNode.textContent = chart && chart.empty_message ? chart.empty_message : 'Нет данных для графика.';
            fallbackNode.classList.remove('is-hidden');
            return;
        }

        fallbackNode.classList.add('is-hidden');
        window.Plotly.react(chartNode, figure.data || [], figure.layout || {}, figure.config || { responsive: true });
    }

    renderChart(data.charts && data.charts.scatter, 'clusterScatterChart', 'clusterScatterChartFallback');
    renderChart(data.charts && data.charts.distribution, 'clusterDistributionChart', 'clusterDistributionChartFallback');
    renderChart(data.charts && data.charts.diagnostics, 'clusterDiagnosticsChart', 'clusterDiagnosticsChartFallback');
})();
