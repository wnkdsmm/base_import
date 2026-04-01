// API and errors

    function hideDashboardError() {
        const card = byId('dashboardInlineError');
        if (card) {
            card.classList.add('is-hidden');
        }
        setText('dashboardInlineErrorLead', '');
        setText('dashboardInlineErrorMessage', '');
    }

    function showDashboardError(error) {
        const card = byId('dashboardInlineError');
        if (!card) {
            return;
        }

        const statusCode = Number(error && error.dashboardStatusCode ? error.dashboardStatusCode : 0);
        const errorId = error && error.dashboardErrorId ? String(error.dashboardErrorId) : '';
        const baseMessage = error && error.message
            ? String(error.message)
            : 'Не удалось обновить панель. Попробуйте повторить запрос.';
        const lead = statusCode >= 500
            ? 'Не удалось обновить панель'
            : statusCode >= 400
                ? 'Проверьте параметры запроса'
                : 'Не удалось загрузить данные';
        const fullMessage = errorId ? baseMessage + ' Код ошибки: ' + errorId + '.' : baseMessage;

        setText('dashboardInlineErrorLead', lead);
        setText('dashboardInlineErrorMessage', fullMessage);
        card.classList.remove('is-hidden');
    }

    function buildDashboardApiError(response, payload) {
        const errorPayload = payload && payload.error ? payload.error : {};
        const statusCode = Number(errorPayload.status_code || (response && response.status) || 0);
        const message = errorPayload.message || (
            statusCode >= 500
                ? 'Не удалось обновить dashboard. Попробуйте повторить запрос.'
                : 'Не удалось обработать параметры dashboard.'
        );
        const error = new Error(message);
        error.dashboardStatusCode = statusCode;
        error.dashboardErrorId = errorPayload.error_id || '';
        error.dashboardCode = errorPayload.code || '';
        return error;
    }

    async function readDashboardResponse(response) {
        let payload = null;

        try {
            payload = await response.json();
        } catch (parseError) {
            payload = null;
        }

        if (!response.ok || (payload && payload.ok === false)) {
            throw buildDashboardApiError(response, payload);
        }

        if (!payload || typeof payload !== 'object') {
            const contractError = new Error('Dashboard API вернул пустой ответ.');
            contractError.dashboardStatusCode = 502;
            throw contractError;
        }

        if (payload.bootstrap_mode === 'deferred') {
            const contractError = new Error('Dashboard API вернул shell вместо готовых данных.');
            contractError.dashboardStatusCode = 502;
            throw contractError;
        }

        return payload;
    }

    async function fetchDashboardData() {
        const form = byId('filtersForm');
        const button = byId('refreshDashboardButton');
        if (!form) {
            return;
        }

        const params = new URLSearchParams(new FormData(form));
        const query = params.toString();

        if (button) {
            button.disabled = true;
        }

        try {
            hideDashboardError();
            const response = await fetch('/api/dashboard-data?' + query, {
                headers: {
                    'Accept': 'application/json'
                }
            });

            /* response status handled in readDashboardResponse
                throw new Error('Не удалось обновить панель');
            */

            const data = await readDashboardResponse(response);
            applyDashboardData(data);
            window.history.replaceState({}, '', buildDashboardPageHref(collectDashboardFiltersFromForm()));
        } catch (error) {
            console.error(error);
            showDashboardError(error);
        } finally {
            if (button) {
                button.disabled = false;
            }
        }
    }
