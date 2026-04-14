@echo off
setlocal EnableExtensions

set "PROJECT_ROOT=%~dp0"
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"
set "SERVER_TITLE=Fire Data FastAPI"
set "VENV_PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "ENV_FILE=%PROJECT_ROOT%\.env"

if not exist "%VENV_PYTHON%" (
    echo Сначала запустите setup.bat для установки зависимостей
    pause
    exit /b 1
)

if not exist "%ENV_FILE%" (
    echo Файл .env не найден. Запустите setup.bat
    pause
    exit /b 1
)

for /f "usebackq tokens=1,2 delims==" %%A in (`findstr /R /V "^[ ]*# ^[ ]*$" "%ENV_FILE%"`) do (
    set "%%A=%%B"
)

if not defined APP_HOST set "APP_HOST=127.0.0.1"
if not defined APP_PORT set "APP_PORT=8000"
set "APP_BASE_URL=http://%APP_HOST%:%APP_PORT%"
set "APP_URL=http://%APP_HOST%:%APP_PORT%/"
set "READY_URL=http://%APP_HOST%:%APP_PORT%/favicon.ico"

call :wait_for_url
if "%errorlevel%"=="0" (
    start "" "%APP_URL%"
    exit /b 0
)

call :resolve_server_command
if not "%errorlevel%"=="0" (
    echo Could not find Python or uvicorn to start the FastAPI server.
    echo Set APP_PYTHON or create .venv\Scripts\python.exe if you want to pin a different interpreter.
    pause
    exit /b 1
)

echo Запуск Fire Analytics Dashboard...
echo Адрес: %APP_BASE_URL%
echo Для остановки закройте это окно
start "%SERVER_TITLE%" cmd /k "cd /d ""%PROJECT_ROOT%"" && %SERVER_COMMAND%"

call :wait_for_url
if not "%errorlevel%"=="0" (
    echo Server did not respond on %READY_URL% within 30 seconds.
    echo Check the "%SERVER_TITLE%" window for the error details.
    pause
    exit /b 1
)

start "" "%APP_URL%"
exit /b 0

:resolve_server_command
set "SERVER_COMMAND="

if defined APP_PYTHON if exist "%APP_PYTHON%" (
    set "SERVER_COMMAND=""%APP_PYTHON%"" -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)

if exist "%VENV_PYTHON%" (
    set "SERVER_COMMAND=""%VENV_PYTHON%"" -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)

where uvicorn >nul 2>nul
if not errorlevel 1 (
    set "SERVER_COMMAND=uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)

where py >nul 2>nul
if not errorlevel 1 (
    set "SERVER_COMMAND=py -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)

where python >nul 2>nul
if not errorlevel 1 (
    set "SERVER_COMMAND=python -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)

call :resolve_local_python "%LocalAppData%\Programs\Python\Python*\python.exe"
if "%errorlevel%"=="0" exit /b 0

call :resolve_local_python "%LocalAppData%\Python\*\python.exe"
if "%errorlevel%"=="0" exit /b 0

exit /b 1

:resolve_local_python
for /f "delims=" %%I in ('dir /b /s "%~1" 2^>nul') do (
    set "SERVER_COMMAND=""%%~fI"" -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
    exit /b 0
)
exit /b 1

:wait_for_url
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ProgressPreference = 'SilentlyContinue';" ^
  "$deadline = (Get-Date).AddSeconds(30);" ^
  "while ((Get-Date) -lt $deadline) {" ^
  "  try {" ^
  "    $response = Invoke-WebRequest -Uri '%READY_URL%' -UseBasicParsing -TimeoutSec 2;" ^
  "    if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) { exit 0 }" ^
  "  } catch { }" ^
  "  Start-Sleep -Milliseconds 500;" ^
  "}" ^
  "exit 1"
exit /b %errorlevel%
