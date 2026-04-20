@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
set "REQ=%ROOT%requirements.txt"
set "WHEELS=%ROOT%wheels"
set "VENV=%ROOT%.venv"
set "PIP=%VENV%\Scripts\pip.exe"
set "PYTHON=%VENV%\Scripts\python.exe"
set "LOG=%ROOT%offline_install.log"

echo ==========================================
echo Офлайн-установка зависимостей
echo ==========================================
echo Папка проекта: %ROOT%
echo requirements.txt: %REQ%
echo Папка wheels: %WHEELS%
echo.

if not exist "%REQ%" (
    echo [ОШИБКА] Не найден requirements.txt: %REQ%
    pause
    exit /b 1
)

if not exist "%WHEELS%" (
    echo [ОШИБКА] Не найдена папка wheels\: %WHEELS%
    echo Скопируйте wheels\ рядом с этим bat-файлом.
    pause
    exit /b 1
)

if not exist "%VENV%\Scripts\python.exe" (
    echo [ШАГ] Создаю виртуальное окружение .venv ...
    py -m venv "%VENV%"
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать .venv (команда: py -m venv .venv)
        echo Проверьте установлен ли Python Launcher (py) и версия Python.
        pause
        exit /b 1
    )
) else (
    echo [OK] .venv уже существует.
)

if not exist "%PIP%" (
    echo [ОШИБКА] Не найден pip в .venv: %PIP%
    pause
    exit /b 1
)

echo.
echo [ШАГ] Установка из локальной папки wheels\ без интернета...
echo [ИНФО] Подробный лог: %LOG%

"%PIP%" install --no-index --find-links="%WHEELS%" -r "%REQ%" >"%LOG%" 2>&1
set "INSTALL_CODE=%ERRORLEVEL%"

if not "%INSTALL_CODE%"=="0" (
    echo [ОШИБКА] Установка завершилась с ошибкой (код %INSTALL_CODE%).
    echo.
    echo Возможная отсутствующая зависимость:
    set "MISSING_LINE="
    for /f "usebackq delims=" %%L in (`findstr /i /c:"No matching distribution found for" "%LOG%"`) do (
        set "MISSING_LINE=%%L"
    )
    if defined MISSING_LINE (
        echo !MISSING_LINE!
    ) else (
        echo Не удалось автоматически определить пакет.
    )
    echo.
    echo Последние сообщения pip:
    for /f "usebackq delims=" %%L in (`powershell -NoProfile -Command "Get-Content -LiteralPath '%LOG%' -Tail 30"`) do echo %%L
    echo.
    echo [ПОДСКАЗКА] Обычно это значит, что в wheels\ нет нужного wheel для вашей версии Python/архитектуры.
    pause
    exit /b %INSTALL_CODE%
)

echo [OK] Пакеты установлены.
echo.
echo [ШАГ] Проверка импортов...
"%PYTHON%" -c "import fastapi, pandas, sklearn; print('OK')"
if errorlevel 1 (
    echo [ОШИБКА] Проверка импортов не прошла.
    pause
    exit /b 1
)

echo ==========================================
echo Установка завершена успешно.
echo ==========================================
pause
exit /b 0

