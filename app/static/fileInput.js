const fileInput = document.getElementById("fileInput");

// Функция для кнопки Import Data
function importData() {
    fileInput.click(); // открываем окно выбора файла
}

// Когда пользователь выбрал файл
fileInput.onchange = async function() {
    if(fileInput.files.length === 0) return;

    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    // Загружаем файл на сервер
    await fetch("/upload", {
        method: "POST",
        body: formData
    });

    // После загрузки вызываем ImportDataStep
    await fetch("/run_import_data", {method:"POST"});
}

// Функция запуска pipeline
async function runPipeline(type){
    let endpoint = "/run_pipeline";
    if(type === "firemap") endpoint = "/run_fire_map";
    else if(type === "features") endpoint = "/run_feature_selection";

    await fetch(endpoint,{method:"POST"});
}

// Обновление логов
async function updateLogs(){
    const res = await fetch("/logs");
    const data = await res.json();
    document.getElementById("logs").innerHTML = data.logs.join("<br>");
}
setInterval(updateLogs,1000);