(function (global) {
    function bootstrapMlModelPage() {
        if (!global.MlModelUi || typeof global.MlModelUi.init !== 'function') {
            return;
        }
        global.MlModelUi.init();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bootstrapMlModelPage);
        return;
    }
    bootstrapMlModelPage();
}(window));
