// Forms and navigation

    function buildQueryFromForm() {
        var form = byId('mlModelForm');
        if (!form) {
            return '';
        }
        return new URLSearchParams(new FormData(form)).toString();
    }

    function buildPayloadFromQuery(query) {
        var params = new URLSearchParams(query || '');
        return {
            table_name: params.get('table_name') || 'all',
            cause: params.get('cause') || 'all',
            object_category: params.get('object_category') || 'all',
            temperature: params.get('temperature') || '',
            forecast_days: params.get('forecast_days') || '14',
            history_window: params.get('history_window') || 'all'
        };
    }

    function collectMlFiltersFromForm() {
        return {
            table_name: byId('mlTableFilter') ? byId('mlTableFilter').value : 'all',
            cause: byId('mlCauseFilter') ? byId('mlCauseFilter').value : 'all',
            object_category: byId('mlObjectCategoryFilter') ? byId('mlObjectCategoryFilter').value : 'all',
            temperature: byId('mlTemperatureInput') ? byId('mlTemperatureInput').value : '',
            forecast_days: byId('mlForecastDaysFilter') ? byId('mlForecastDaysFilter').value : '14',
            history_window: byId('mlHistoryWindowFilter') ? byId('mlHistoryWindowFilter').value : 'all'
        };
    }

    function buildMlNavigationHref(path, filters, options) {
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

    function updateMlScreenLinks(filters) {
        var safeFilters = filters || collectMlFiltersFromForm();
        setHref('mlPanelLink', buildMlNavigationHref('/', safeFilters, { onlyTable: true }));
        setHref('mlScenarioLink', buildMlNavigationHref('/forecasting', safeFilters));
        setHref('mlDecisionLink', buildMlNavigationHref('/forecasting', safeFilters, { hash: '#forecastDetails' }));
    }

    function buildRequestPayload(options) {
        var settings = options || {};
        var query = settings.useLocationSearch && window.location.search
            ? window.location.search.replace(/^\?/, '')
            : buildQueryFromForm();
        return {
            query: query,
            body: buildPayloadFromQuery(query)
        };
    }
