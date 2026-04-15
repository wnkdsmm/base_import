(function (global) {
    var factory = global.FireStateFactory || {};
    var createStateManager = factory.createStateManager;

    global.AccessPointsState = {
        create: function createAccessPointsState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var fallbackCurrentController = null;
            var fallbackLatestRequestId = 0;
            var manager = typeof createStateManager === 'function'
                ? createStateManager({
                    currentController: null,
                    initialData: initialData,
                    latestRequestId: 0
                })
                : null;

            function getInitialData() {
                return manager ? manager.get('initialData') : initialData;
            }

            function getCurrentController() {
                return manager ? manager.get('currentController') : fallbackCurrentController;
            }

            function setCurrentController(controller) {
                if (!manager) {
                    fallbackCurrentController = controller || null;
                    return fallbackCurrentController;
                }
                return manager.set('currentController', controller || null);
            }

            function clearController(controller) {
                var currentController = getCurrentController();
                if (currentController === controller) {
                    setCurrentController(null);
                }
                return getCurrentController();
            }

            function nextRequestId() {
                if (!manager) {
                    fallbackLatestRequestId += 1;
                    return fallbackLatestRequestId;
                }
                return manager.set('latestRequestId', Number(manager.get('latestRequestId') || 0) + 1);
            }

            function isLatestRequest(requestId) {
                var latestRequestId = manager ? manager.get('latestRequestId') : fallbackLatestRequestId;
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
