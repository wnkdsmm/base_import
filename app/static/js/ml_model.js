(function (global) {
    function boot() {
        if (!global.MlModelEvents || typeof global.MlModelEvents.bootstrap !== 'function') {
            return;
        }
        global.MlModelEvents.bootstrap();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', boot);
        return;
    }
    boot();
}(window));

