async function selectAndImport() {
        const fileInput = document.getElementById("fileInput");
        
        // Слушаем событие выбора файла
        fileInput.onchange = async () => {
            const file = fileInput.files[0];
            if (!file) return;

            // Показываем индикатор загрузки (опционально)
            const logBox = document.getElementById("logs");
            if (logBox) {
                logBox.innerHTML = `<div>⏳ Загрузка файла ${file.name}...</div>`;
            }

            // 1️⃣ Загружаем файл на сервер
            const uploadData = new FormData();
            uploadData.append("file", file);

            let uploadResponse = await fetch("/upload", {
                method: "POST",
                body: uploadData
            });
            
            if (!uploadResponse.ok) {
                if (logBox) {
                    logBox.innerHTML += `<div>❌ Ошибка при загрузке файла</div>`;
                }
                return;
            }
            
            let uploadResult = await uploadResponse.json();
            if (uploadResult.status !== "uploaded") {
                if (logBox) {
                    logBox.innerHTML += `<div>❌ Ошибка при загрузке файла</div>`;
                }
                return;
            }

            if (logBox) {
                logBox.innerHTML += `<div>✅ Файл загружен, начинаем импорт...</div>`;
            }

            // 2️⃣ Запускаем импорт (output_folder не обязателен)
            const importForm = new FormData();

            let importResponse = await fetch("/import_data", {
                method: "POST",
                body: importForm
            });

            if (!importResponse.ok) {
                if (logBox) {
                    logBox.innerHTML += `<div>❌ Ошибка при импорте данных</div>`;
                }
                return;
            }

            let importResult = await importResponse.json();
            
            // Показываем результат в логах вместо alert
            let message = importResult.status;
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
                logBox.innerHTML += `<div>${message}</div>`;
            }
            console.log(importResult);
            
            // Обновляем логи с сервера
            await refreshLogs();

            if (window.fireDashboard && typeof window.fireDashboard.afterImport === "function") {
                window.fireDashboard.afterImport();
            }
        };

        // Открываем диалог выбора файла
        fileInput.click();
    }

    // Функция для обновления логов
    async function refreshLogs() {
        const logBox = document.getElementById("logs");
        if (!logBox) {
            return;
        }

        try {
            let response = await fetch("/logs");
            let logs = await response.json();
            logBox.innerHTML = logs.logs.map(log => `<div>${log}</div>`).join('');
            // Автопрокрутка вниз
            logBox.scrollTop = logBox.scrollHeight;
        } catch (error) {
            console.error("Error refreshing logs:", error);
        }
    }

    // Автоматически обновляем логи каждые 2 секунды
    setInterval(refreshLogs, 2000);

    // Также обновляем логи сразу при загрузке страницы
    document.addEventListener('DOMContentLoaded', refreshLogs);
