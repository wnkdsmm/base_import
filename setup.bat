@echo off
setlocal EnableExtensions
chcp 65001 >nul

set "ROOT=%~dp0"
cd /d "%ROOT%"

echo ==========================================
echo Установка Fire Data
echo ==========================================

set "PY_CMD="
where py >nul 2>&1 && set "PY_CMD=py"
if not defined PY_CMD (
    where python >nul 2>&1 && set "PY_CMD=python"
)

if not defined PY_CMD (
    echo [ОШИБКА] Python не найден в PATH.
    echo Установите Python 3.10+ и добавьте его в PATH.
    pause
    exit /b 1
)

echo [1/5] Использую: %PY_CMD%

if not exist ".venv\Scripts\python.exe" (
    echo [2/5] Создаю .venv ...
    %PY_CMD% -m venv .venv
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать .venv
        pause
        exit /b 1
    )
) else (
    echo [2/5] .venv уже существует.
)

echo [3/5] Устанавливаю зависимости ...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
    echo [ОШИБКА] Не удалось обновить pip
    pause
    exit /b 1
)
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo [ОШИБКА] Не удалось установить зависимости
    pause
    exit /b 1
)

if not exist ".env" (
    echo [4/5] Создаю .env из .env.example ...
    if not exist ".env.example" (
        echo [ОШИБКА] Не найден .env.example
        pause
        exit /b 1
    )
    copy /Y ".env.example" ".env" >nul
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать .env
        pause
        exit /b 1
    )
) else (
    echo [4/5] .env уже существует.
)

echo [5/5] Проверка импортов ...
".venv\Scripts\python.exe" -c "import fastapi,uvicorn,pandas,sqlalchemy; print('OK')"
if errorlevel 1 (
    echo [ОШИБКА] Проверка импортов не прошла
    pause
    exit /b 1
)

echo.
echo [OK] Готово.
echo Запуск приложения: start_web_app.vbs
pause
exit /b 0
