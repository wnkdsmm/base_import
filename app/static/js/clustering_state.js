(function (global) {
    var shared = global.FireUi || {};
    var factory = global.FireStateFactory || {};
    var createStateManager = factory.createStateManager;

    global.ClusteringState = {
        create: function createClusteringState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var manager = typeof createStateManager === 'function'
                ? createStateManager({ initialData: initialData })
                : null;
            var createSingleTimer = shared.createSingleTimer;
            var pollTimer = typeof createSingleTimer === 'function' ? createSingleTimer() : {
                clear: function () {},
                set: function (handler, delay) {
                    setTimeout(handler, delay);
                }
            };

            function getInitialData() {
                return manager ? manager.get('initialData') : initialData;
            }

            function clearPollTimer() {
                pollTimer.clear();
            }

            function setPollTimer(handler, delay) {
                pollTimer.set(handler, delay);
            }

            return {
                clearPollTimer: clearPollTimer,
                getInitialData: getInitialData,
                setPollTimer: setPollTimer
            };
        }
    };
}(window));
