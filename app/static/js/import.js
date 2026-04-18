const LOG_REFRESH_INTERVAL_MS = 2000;
const IMPORT_JOB_STORAGE_KEY = "fire-monitor-import-job-id";
const shared = window.FireUi;
const createJobId = shared.createJobId;
const fetchJson = shared.fetchJson;
const getApiErrorMessage = shared.getApiErrorMessage;
const logsRefreshTimers = shared.createTimerGroup();
let logsRefreshStarted = false;
let currentImportJobId = null;

function createLogLine(text) {
    const line = document.createElement("div");
    line.textContent = text;
    return line;
}

function replaceLogLines(logBox, items) {
    logBox.replaceChildren(...items.map((item) => createLogLine(String(item ?? ""))));
}

function appendLogLine(logBox, text) {
    logBox.appendChild(createLogLine(text));
}

function appendApiError(logBox, error, fallback) {
    if (!logBox) {
        return;
    }
    appendLogLine(logBox, getApiErrorMessage(error && error.payload, fallback));
}

function setCurrentImportJobId(jobId) {
    currentImportJobId = jobId || null;
    if (!window.sessionStorage) {
        return;
    }
    if (currentImportJobId) {
        window.sessionStorage.setItem(IMPORT_JOB_STORAGE_KEY, currentImportJobId);
    } else {
        window.sessionStorage.removeItem(IMPORT_JOB_STORAGE_KEY);
    }
}

function getCurrentImportJobId() {
    if (currentImportJobId) {
        return currentImportJobId;
    }
    if (!window.sessionStorage) {
        return null;
    }
    const storedJobId = window.sessionStorage.getItem(IMPORT_JOB_STORAGE_KEY);
    if (storedJobId) {
        currentImportJobId = storedJobId;
        return storedJobId;
    }
    return null;
}

async function refreshLogs(jobId = getCurrentImportJobId()) {
    const logBox = document.getElementById("logs");
    if (!logBox) {
        return;
    }

    try {
        const url = jobId ? `/logs?job_id=${encodeURIComponent(jobId)}` : "/logs";
        const result = await fetchJson(url, {
            headers: {
                "Accept": "application/json",
            },
        }, "Не удалось обновить лог импорта.");
        const payload = result.payload || {};
        const items = Array.isArray(payload.logs) ? payload.logs : [];
        replaceLogLines(logBox, items);
        logBox.scrollTop = logBox.scrollHeight;
    } catch (error) {
        console.error("Error refreshing logs:", error);
    }
}

function initializeImportLogs() {
    const logBox = document.getElementById("logs");
    if (!logBox) {
        return;
    }

    refreshLogs();
    if (!logsRefreshStarted) {
        logsRefreshStarted = true;
        const pollLogs = async () => {
            await refreshLogs();
            logsRefreshTimers.set(pollLogs, LOG_REFRESH_INTERVAL_MS);
        };
        logsRefreshTimers.set(pollLogs, LOG_REFRESH_INTERVAL_MS);
    }
}

function notifyImportComplete(importResult) {
    try {
        document.dispatchEvent(new CustomEvent("fire-monitor:import-complete", {
            detail: importResult || {},
        }));
    } catch (error) {
        console.error("Error dispatching import completion event:", error);
    }

    const hooks = [
        window.fireDashboard && window.fireDashboard.afterImport,
        window.fireTables && window.fireTables.afterImport,
    ];

    hooks.forEach((hook) => {
        if (typeof hook === "function") {
            try {
                hook(importResult || {});
            } catch (error) {
                console.error("Error in afterImport hook:", error);
            }
        }
    });
}

async function selectAndImport() {
    const fileInput = document.getElementById("fileInput");
    if (!fileInput) {
        return;
    }

    fileInput.value = "";
    fileInput.onchange = async () => {
        const file = fileInput.files && fileInput.files[0];
        if (!file) {
            return;
        }

        const jobId = createJobId();
        setCurrentImportJobId(jobId);

        const logBox = document.getElementById("logs");
        if (logBox) {
            replaceLogLines(logBox, [`Загрузка файла ${file.name}...`]);
        }

        const uploadData = new FormData();
        uploadData.append("file", file);
        uploadData.append("job_id", jobId);

        let uploadResult = null;
        try {
            const uploadApiResult = await fetchJson("/upload", {
                method: "POST",
                body: uploadData,
            }, "Ошибка при загрузке файла");
            uploadResult = uploadApiResult.payload || {};
        } catch (error) {
            appendApiError(logBox, error, "Ошибка при загрузке файла");
            return;
        }

        const resolvedJobId = uploadResult.job_id || jobId;
        setCurrentImportJobId(resolvedJobId);

        if (uploadResult.status !== "uploaded") {
            if (logBox) {
                appendLogLine(logBox, "Ошибка при загрузке файла");
            }
            return;
        }

        if (logBox) {
            appendLogLine(logBox, "Файл загружен, начинаем импорт...");
        }

        await refreshLogs(resolvedJobId);

        const importData = new FormData();
        importData.append("job_id", resolvedJobId);

        let importResult = null;
        try {
            const importApiResult = await fetchJson("/import_data", {
                method: "POST",
                body: importData,
            }, "Ошибка при импорте данных");
            importResult = importApiResult.payload || {};
        } catch (error) {
            appendApiError(logBox, error, "Ошибка при импорте данных");
            return;
        }

        let message = importResult.status || "Импорт завершен";
        if (importResult.rows) {
            message += ` (${importResult.rows} строк, ${importResult.columns} колонок)`;
        }
        if (importResult.project_name) {
            message += ` | Проект: ${importResult.project_name}`;
        }
        if (importResult.output_folder) {
            message += ` | Папка: ${importResult.output_folder}`;
        }

        if (logBox) {
            appendLogLine(logBox, message);
        }

        await refreshLogs(resolvedJobId);

        notifyImportComplete(importResult);
    };

    fileInput.click();
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeImportLogs, { once: true });
} else {
    initializeImportLogs();
}
