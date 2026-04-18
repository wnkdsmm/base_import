(function (global) {
    var shared = global.FireUi || {};
    var factory = global.FireStateFactory || {};
    var createModuleState = factory.createModuleState;

    global.ClusteringState = {
        create: function createClusteringState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var state = typeof createModuleState === 'function'
                ? createModuleState('clustering', { initialData: initialData })
                : null;
            var fallbackInitialData = initialData;
            var createSingleTimer = shared.createSingleTimer;
            var pollTimer = typeof createSingleTimer === 'function' ? createSingleTimer() : {
                clear: function () {},
                set: function (handler, delay) {
                    setTimeout(handler, delay);
                }
            };

            function getInitialData() {
                return state ? state.get('initialData') : fallbackInitialData;
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
