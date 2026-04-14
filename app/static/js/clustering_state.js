(function (global) {
    var shared = global.FireUi || {};

    global.ClusteringState = {
        create: function createClusteringState(options) {
            var initialData = options && options.initialData ? options.initialData : null;
            var createSingleTimer = shared.createSingleTimer;
            var pollTimer = typeof createSingleTimer === 'function' ? createSingleTimer() : {
                clear: function () {},
                set: function (handler, delay) {
                    setTimeout(handler, delay);
                }
            };

            function getInitialData() {
                return initialData;
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
