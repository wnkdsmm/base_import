(function () {
    var shared = window.FireUi;
    if (!shared) {
        return;
    }

    var modules = window.ForecastingModules || {};
    var createCharts = modules.createForecastingCharts;
    var createUi = modules.createForecastingUi;
    var createApi = modules.createForecastingApi;
    var initPage = modules.initForecastingPage;

    if (
        typeof createCharts !== 'function' ||
        typeof createUi !== 'function' ||
        typeof createApi !== 'function' ||
        typeof initPage !== 'function'
    ) {
        return;
    }

    document.addEventListener('DOMContentLoaded', function () {
        var charts = createCharts();
        var ui = createUi({ charts: charts });
        var api = createApi({ ui: ui });
        initPage({ api: api, ui: ui });
    });
})();

