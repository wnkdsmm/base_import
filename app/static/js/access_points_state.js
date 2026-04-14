(function (global) {
    global.AccessPointsState = {
        create: function createAccessPointsState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var currentController = null;
            var latestRequestId = 0;

            function getInitialData() {
                return initialData;
            }

            function getCurrentController() {
                return currentController;
            }

            function setCurrentController(controller) {
                currentController = controller || null;
                return currentController;
            }

            function clearController(controller) {
                if (currentController === controller) {
                    currentController = null;
                }
                return currentController;
            }

            function nextRequestId() {
                latestRequestId += 1;
                return latestRequestId;
            }

            function isLatestRequest(requestId) {
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
