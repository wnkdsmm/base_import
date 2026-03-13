async function selectAndImport() {
        const fileInput = document.getElementById("fileInput");
        
        // РЎР»СѓС€Р°РµРј СЃРѕР±С‹С‚РёРµ РІС‹Р±РѕСЂР° С„Р°Р№Р»Р°
        fileInput.onchange = async () => {
            const file = fileInput.files[0];
            if (!file) return;

            // РџРѕРєР°Р·С‹РІР°РµРј РёРЅРґРёРєР°С‚РѕСЂ Р·Р°РіСЂСѓР·РєРё (РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ)
            const logBox = document.getElementById("logs");
            logBox.innerHTML = `<div>вЏі Р—Р°РіСЂСѓР·РєР° С„Р°Р№Р»Р° ${file.name}...</div>`;

            // 1пёЏвѓЈ Р—Р°РіСЂСѓР¶Р°РµРј С„Р°Р№Р» РЅР° СЃРµСЂРІРµСЂ
            const uploadData = new FormData();
            uploadData.append("file", file);

            let uploadResponse = await fetch("/upload", {
                method: "POST",
                body: uploadData
            });
            
            if (!uploadResponse.ok) {
                logBox.innerHTML += `<div>вќЊ РћС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ С„Р°Р№Р»Р°</div>`;
                return;
            }
            
            let uploadResult = await uploadResponse.json();
            if (uploadResult.status !== "uploaded") {
                logBox.innerHTML += `<div>вќЊ РћС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ С„Р°Р№Р»Р°</div>`;
                return;
            }

            logBox.innerHTML += `<div>вњ… Р¤Р°Р№Р» Р·Р°РіСЂСѓР¶РµРЅ, РЅР°С‡РёРЅР°РµРј РёРјРїРѕСЂС‚...</div>`;

            // 2пёЏвѓЈ Р—Р°РїСѓСЃРєР°РµРј РёРјРїРѕСЂС‚ (output_folder РЅРµ РѕР±СЏР·Р°С‚РµР»РµРЅ)
            const importForm = new FormData();

            let importResponse = await fetch("/import_data", {
                method: "POST",
                body: importForm
            });

            if (!importResponse.ok) {
                logBox.innerHTML += `<div>вќЊ РћС€РёР±РєР° РїСЂРё РёРјРїРѕСЂС‚Рµ РґР°РЅРЅС‹С…</div>`;
                return;
            }

            let importResult = await importResponse.json();
            
            // РџРѕРєР°Р·С‹РІР°РµРј СЂРµР·СѓР»СЊС‚Р°С‚ РІ Р»РѕРіР°С… РІРјРµСЃС‚Рѕ alert
            let message = importResult.status;
            if (importResult.rows) {
                message += ` (${importResult.rows} СЃС‚СЂРѕРє, ${importResult.columns} РєРѕР»РѕРЅРѕРє)`;
            }
            if (importResult.project_name) {
                message += ` | РџСЂРѕРµРєС‚: ${importResult.project_name}`;
            }
            if (importResult.output_folder) {
                message += ` | РџР°РїРєР°: ${importResult.output_folder}`;
            }
            
            logBox.innerHTML += `<div>${message}</div>`;
            console.log(importResult);
            
            // РћР±РЅРѕРІР»СЏРµРј Р»РѕРіРё СЃ СЃРµСЂРІРµСЂР°
            await refreshLogs();

            if (window.fireDashboard && typeof window.fireDashboard.afterImport === "function") {
                window.fireDashboard.afterImport();
            }
        };

        // РћС‚РєСЂС‹РІР°РµРј РґРёР°Р»РѕРі РІС‹Р±РѕСЂР° С„Р°Р№Р»Р°
        fileInput.click();
    }

    // Р¤СѓРЅРєС†РёСЏ РґР»СЏ РѕР±РЅРѕРІР»РµРЅРёСЏ Р»РѕРіРѕРІ
    async function refreshLogs() {
        try {
            let response = await fetch("/logs");
            let logs = await response.json();
            const logBox = document.getElementById("logs");
            if (logBox) {
                logBox.innerHTML = logs.logs.map(log => `<div>${log}</div>`).join('');
                // РђРІС‚РѕРїСЂРѕРєСЂСѓС‚РєР° РІРЅРёР·
                logBox.scrollTop = logBox.scrollHeight;
            }
        } catch (error) {
            console.error("Error refreshing logs:", error);
        }
    }

    // РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРё РѕР±РЅРѕРІР»СЏРµРј Р»РѕРіРё РєР°Р¶РґС‹Рµ 2 СЃРµРєСѓРЅРґС‹
    setInterval(refreshLogs, 2000);

    // РўР°РєР¶Рµ РѕР±РЅРѕРІР»СЏРµРј Р»РѕРіРё СЃСЂР°Р·Сѓ РїСЂРё Р·Р°РіСЂСѓР·РєРµ СЃС‚СЂР°РЅРёС†С‹
    document.addEventListener('DOMContentLoaded', refreshLogs);
