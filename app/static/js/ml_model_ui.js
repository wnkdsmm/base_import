(function (global) {
    var render = global.MlModelRender || {};
    var stateFactory = global.MlModelState && typeof global.MlModelState.create === 'function'
        ? global.MlModelState.create
        : null;
    var state = stateFactory ? stateFactory({ initialData: global.__FIRE_ML_INITIAL__ || null }) : null;

    var applyFn = typeof render.applyMlModelData === 'function' ? render.applyMlModelData.bind(render) : function () {};
    var collectFn = typeof render.collectMlFiltersFromForm === 'function'
        ? render.collectMlFiltersFromForm.bind(render)
        : (state && state.collectSelectedFilters ? state.collectSelectedFilters : function () { return {}; });

    global.MlModelUi = {
        applyMlModelData: function applyMlModelData(data) {
            if (state && typeof state.setCurrentData === 'function') {
                state.setCurrentData(data);
            }
            return applyFn(data);
        },
        collectMlFiltersFromForm: collectFn,
        getCurrentMlData: function getCurrentMlData() {
            if (state && typeof state.getCurrentData === 'function') {
                return state.getCurrentData();
            }
            return null;
        },
        init: typeof render.init === 'function' ? render.init.bind(render) : function () {},
        startMlModelJob: typeof render.startMlModelJob === 'function' ? render.startMlModelJob.bind(render) : function () {},
        updateMlScreenLinks: typeof render.updateMlScreenLinks === 'function' ? render.updateMlScreenLinks.bind(render) : function () {}
    };
}(window));

