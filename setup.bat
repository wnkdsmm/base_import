@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
cd /d "%ROOT%"

echo ==========================================
echo Fire Data setup
echo ==========================================

set "PY_CMD="
where py >nul 2>nul
if not errorlevel 1 set "PY_CMD=py"
if not defined PY_CMD (
    where python >nul 2>nul
    if not errorlevel 1 set "PY_CMD=python"
)

if not defined PY_CMD (
    echo [ERROR] Python is not found in PATH.
    echo Install Python 3.10+ and add it to PATH.
    pause
    exit /b 1
)

echo [1/5] Using: %PY_CMD%

if not exist ".venv\Scripts\python.exe" (
    echo [2/5] Creating .venv ...
    %PY_CMD% -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create .venv
        pause
        exit /b 1
    )
) else (
    echo [2/5] .venv already exists.
)

echo [3/5] Installing dependencies ...
".venv\Scripts\python.exe" -m pip install --upgrade pip --no-cache-dir
if errorlevel 1 (
    echo [ERROR] Failed to upgrade pip
    pause
    exit /b 1
)
".venv\Scripts\python.exe" -m pip install --no-cache-dir -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)

if not exist ".env" (
    echo [4/5] Creating .env from .env.example ...
    if not exist ".env.example" (
        echo [ERROR] .env.example is missing
        pause
        exit /b 1
    )
    copy /Y ".env.example" ".env" >nul
    if errorlevel 1 (
        echo [ERROR] Failed to create .env
        pause
        exit /b 1
    )
) else (
    echo [4/5] .env already exists.
)

echo [5/5] Checking imports ...
".venv\Scripts\python.exe" -c "import fastapi,uvicorn,pandas,sqlalchemy; print('OK')"
if errorlevel 1 (
    echo [ERROR] Import check failed
    pause
    exit /b 1
)

echo.
echo [OK] Done.
echo Start app with: start_web_app.vbs
pause
exit /b 0
