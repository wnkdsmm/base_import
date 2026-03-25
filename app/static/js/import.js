const LOG_REFRESH_INTERVAL_MS = 2000;
const IMPORT_JOB_STORAGE_KEY = "fire-monitor-import-job-id";
let logsRefreshTimer = null;
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

function createJobId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
    }
    return `job-${Date.now()}-${Math.random().toString(16).slice(2)}`;
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
        const response = await fetch(url);
        const payload = await response.json();
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
    if (!logsRefreshTimer) {
        logsRefreshTimer = window.setInterval(() => {
            refreshLogs();
        }, LOG_REFRESH_INTERVAL_MS);
    }
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

        const uploadResponse = await fetch("/upload", {
            method: "POST",
            body: uploadData,
        });

        if (!uploadResponse.ok) {
            if (logBox) {
                appendLogLine(logBox, "Ошибка при загрузке файла");
            }
            return;
        }

        const uploadResult = await uploadResponse.json();
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

        const importResponse = await fetch("/import_data", {
            method: "POST",
            body: importData,
        });

        if (!importResponse.ok) {
            if (logBox) {
                appendLogLine(logBox, "Ошибка при импорте данных");
            }
            return;
        }

        const importResult = await importResponse.json();
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

        if (window.fireDashboard && typeof window.fireDashboard.afterImport === "function") {
            window.fireDashboard.afterImport();
        }
    };

    fileInput.click();
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeImportLogs, { once: true });
} else {
    initializeImportLogs();
}
