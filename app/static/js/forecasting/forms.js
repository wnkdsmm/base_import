// Forms and navigation

    function buildForecastBriefHref(filters) {
        var params = new URLSearchParams();
        var safeFilters = filters || {};

        [
            'table_name',
            'district',
            'cause',
            'object_category',
            'temperature',
            'forecast_days',
            'history_window'
        ].forEach(function (key) {
            var value = safeFilters[key];
            if (value != null && value !== '') {
                params.set(key, value);
            }
        });

        var query = params.toString();
        return '/brief/forecasting.txt' + (query ? '?' + query : '');
    }

    function updateForecastBriefExport(filters) {
        var href = buildForecastBriefHref(filters || {});
        Array.prototype.forEach.call(
            document.querySelectorAll('#decisionSupportPanel .executive-brief-download, #decisionSupportPanel .executive-brief-summary-action'),
            function (link) {
                link.setAttribute('href', href);
            }
        );
    }

    function buildForecastNavigationHref(path, filters, options) {
        var safeFilters = filters || {};
        var settings = options || {};
        var params = new URLSearchParams();

        if (safeFilters.table_name && safeFilters.table_name !== 'all') {
            params.set('table_name', safeFilters.table_name);
        }
        if (!settings.onlyTable) {
            ['cause', 'object_category', 'temperature', 'forecast_days', 'history_window'].forEach(function (key) {
                var value = safeFilters[key];
                if (value != null && value !== '' && value !== 'all') {
                    params.set(key, value);
                }
            });
        }

        var query = params.toString();
        return path + (query ? '?' + query : '') + (settings.hash || '');
    }

    function updateForecastScreenLinks(filters) {
        var safeFilters = filters || collectForecastFiltersFromForm();
        setHref('forecastPanelLink', buildForecastNavigationHref('/', safeFilters, { onlyTable: true }));
        setHref('forecastMlLink', buildForecastNavigationHref('/ml-model', safeFilters));
    }

    function collectForecastFiltersFromForm() {
        return {
            table_name: byId('forecastTableFilter') ? byId('forecastTableFilter').value : '',
            district: byId('forecastDistrictFilter') ? byId('forecastDistrictFilter').value : 'all',
            cause: byId('forecastCauseFilter') ? byId('forecastCauseFilter').value : 'all',
            object_category: byId('forecastObjectCategoryFilter') ? byId('forecastObjectCategoryFilter').value : 'all',
            temperature: byId('forecastTemperatureInput') ? byId('forecastTemperatureInput').value : '',
            forecast_days: byId('forecastDaysFilter') ? byId('forecastDaysFilter').value : '',
            history_window: byId('forecastHistoryWindowFilter') ? byId('forecastHistoryWindowFilter').value : ''
        };
    }
