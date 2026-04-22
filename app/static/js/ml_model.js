(function (global) {
    // Keep this entry file touched when dependent ML modules change,
    // because the template cache key is derived from ml_model.js mtime.
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
