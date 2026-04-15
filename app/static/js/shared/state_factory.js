(function (global) {
    function cloneState(source) {
        var snapshot = {};
        var safeSource = source || {};
        Object.keys(safeSource).forEach(function (key) {
            snapshot[key] = safeSource[key];
        });
        return snapshot;
    }

    function createStateManager(initialState) {
        var state = cloneState(initialState);
        var subscribers = [];

        function notify() {
            var snapshot = cloneState(state);
            subscribers.slice().forEach(function (listener) {
                try {
                    listener(snapshot);
                } catch (error) {
                    setTimeout(function () {
                        throw error;
                    }, 0);
                }
            });
        }

        function get(key) {
            if (typeof key === 'undefined') {
                return cloneState(state);
            }
            return state[key];
        }

        function set(key, value) {
            if (typeof key === 'object' && key !== null) {
                Object.keys(key).forEach(function (field) {
                    state[field] = key[field];
                });
                notify();
                return get();
            }
            state[key] = value;
            notify();
            return value;
        }

        function subscribe(listener) {
            if (typeof listener !== 'function') {
                return function () {};
            }
            subscribers.push(listener);
            return function () {
                subscribers = subscribers.filter(function (item) {
                    return item !== listener;
                });
            };
        }

        return {
            get: get,
            set: set,
            subscribe: subscribe
        };
    }

    global.FireStateFactory = {
        createStateManager: createStateManager
    };
}(window));
