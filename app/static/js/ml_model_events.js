(function (global) {
    global.MlModelEvents = {
        bootstrap: function bootstrapMlModelPage() {
            if (!global.MlModelUi || typeof global.MlModelUi.init !== 'function') {
                return;
            }
            global.MlModelUi.init();
        }
    };
}(window));

