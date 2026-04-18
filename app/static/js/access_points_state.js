(function (global) {
    var factory = global.FireStateFactory || {};
    var createModuleState = factory.createModuleState;

    global.AccessPointsState = {
        create: function createAccessPointsState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var fallbackCurrentController = null;
            var fallbackLatestRequestId = 0;
            var state = typeof createModuleState === 'function'
                ? createModuleState('access_points', {
                    currentController: null,
                    initialData: initialData,
                    latestRequestId: 0
                })
                : null;

            function getInitialData() {
                return state ? state.get('initialData') : initialData;
            }

            function getCurrentController() {
                return state ? state.get('currentController') : fallbackCurrentController;
            }

            function setCurrentController(controller) {
                if (!state) {
                    fallbackCurrentController = controller || null;
                    return fallbackCurrentController;
                }
                return state.set('currentController', controller || null);
            }

            function clearController(controller) {
                var currentController = getCurrentController();
                if (currentController === controller) {
                    setCurrentController(null);
                }
                return getCurrentController();
            }

            function nextRequestId() {
                if (!state) {
                    fallbackLatestRequestId += 1;
                    return fallbackLatestRequestId;
                }
                return state.set('latestRequestId', Number(state.get('latestRequestId') || 0) + 1);
            }

            function isLatestRequest(requestId) {
                var latestRequestId = state ? state.get('latestRequestId') : fallbackLatestRequestId;
                return requestId == latestRequestId;
            }

            return {
                clearController: clearController,
                getCurrentController: getCurrentController,
                getInitialData: getInitialData,
                isLatestRequest: isLatestRequest,
                nextRequestId: nextRequestId,
                setCurrentController: setCurrentController
            };
        }
    };
}(window));
