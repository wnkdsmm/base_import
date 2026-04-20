@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
set "REQ=%ROOT%requirements.txt"
set "WHEELS=%ROOT%wheels"
set "LOG=%ROOT%wheels_download.log"

echo ==========================================
echo Загрузка пакетов для офлайн-установки
echo ==========================================
echo Папка проекта: %ROOT%
echo Файл зависимостей: %REQ%
echo Папка wheels: %WHEELS%
echo.

if not exist "%REQ%" (
    echo [ОШИБКА] Не найден requirements.txt: %REQ%
    pause
    exit /b 1
)

if not exist "%WHEELS%" (
    mkdir "%WHEELS%"
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать папку: %WHEELS%
        pause
        exit /b 1
    )
)

echo [ШАГ] Скачивание бинарных пакетов (wheel)...
echo [ИНФО] Подробный лог: %LOG%
echo.

py -m pip download -r "%REQ%" -d "%WHEELS%" --platform win_amd64 --python-version 3.11 --only-binary=:all: >"%LOG%" 2>&1
set "DL_CODE=%ERRORLEVEL%"

if not "%DL_CODE%"=="0" (
    echo [ПРЕДУПРЕЖДЕНИЕ] pip download завершился с ошибкой (код %DL_CODE%).
    echo Возможно, для части пакетов нет бинарных wheel под win_amd64/Python 3.11.
    echo.
    echo Последние сообщения pip:
    for /f "usebackq delims=" %%L in (`powershell -NoProfile -Command "Get-Content -LiteralPath '%LOG%' -Tail 30"`) do echo %%L
    echo.
    echo [РЕКОМЕНДАЦИЯ] Проверьте лог и при необходимости скорректируйте версии пакетов.
) else (
    echo [OK] Загрузка завершена без ошибок.
)

set /a FILE_COUNT=0
for /f %%C in ('dir /a:-d /b "%WHEELS%" ^| find /v /c ""') do set "FILE_COUNT=%%C"

set "TOTAL_BYTES=0"
for /f "delims=" %%S in ('powershell -NoProfile -Command "(Get-ChildItem -LiteralPath '%WHEELS%' -File -Recurse ^| Measure-Object -Property Length -Sum).Sum"') do set "TOTAL_BYTES=%%S"
if not defined TOTAL_BYTES set "TOTAL_BYTES=0"

for /f "delims=" %%H in ('powershell -NoProfile -Command "$b=[double]('%TOTAL_BYTES%'); if($b -ge 1GB){'{0:N2} ГБ' -f ($b/1GB)} elseif($b -ge 1MB){'{0:N2} МБ' -f ($b/1MB)} elseif($b -ge 1KB){'{0:N2} КБ' -f ($b/1KB)} else {'{0:N0} байт' -f $b}"') do set "HUMAN_SIZE=%%H"

echo.
echo ==========================================
echo ИТОГ:
echo Файлов в wheels\: %FILE_COUNT%
echo Общий размер: %HUMAN_SIZE%
echo ==========================================

if not "%DL_CODE%"=="0" (
    echo [ВНИМАНИЕ] Есть проблемы со скачиванием. Перед переносом проверьте wheels_download.log.
    pause
    exit /b %DL_CODE%
)

echo Готово. Можно переносить папку wheels\ и служебные файлы на новое устройство.
pause
exit /b 0

