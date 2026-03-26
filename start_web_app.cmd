@echo off
setlocal EnableExtensions

set "PROJECT_ROOT=%~dp0"
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"
set "APP_HOST=127.0.0.1"
set "APP_PORT=8000"
set "APP_URL=http://%APP_HOST%:%APP_PORT%/"
set "READY_URL=http://%APP_HOST%:%APP_PORT%/favicon.ico"
set "SERVER_TITLE=Fire Data FastAPI"

call :wait_for_url
if "%errorlevel%"=="0" (
    start "" "%APP_URL%"
    exit /b 0
)

call :resolve_server_command
if not "%errorlevel%"=="0" (
    echo Could not find Python or uvicorn to start the FastAPI server.
    echo Edit start_web_app.cmd if you want to pin a different interpreter path.
    pause
    exit /b 1
)

echo Starting FastAPI server in a new window...
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

if exist "%LocalAppData%\Python\pythoncore-3.14-64\python.exe" (
    set "SERVER_COMMAND=""%LocalAppData%\Python\pythoncore-3.14-64\python.exe"" -m uvicorn app.main:app --host %APP_HOST% --port %APP_PORT% --reload"
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
