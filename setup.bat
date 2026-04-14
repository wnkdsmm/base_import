@echo off
setlocal EnableExtensions
chcp 65001 >nul
title Установка Fire Analytics Dashboard

echo === Установка Fire Analytics Dashboard ===
echo.

set "PY_LAUNCHER="

echo [1/6] Проверка Python...
py --version >nul 2>&1
if %errorlevel%==0 (
    set "PY_LAUNCHER=py"
) else (
    python --version >nul 2>&1
    if %errorlevel%==0 (
        set "PY_LAUNCHER=python"
    ) else (
        echo ОШИБКА: Python не найден.
        echo Установите Python 3.9+ с https://python.org/downloads/
        pause
        exit /b 1
    )
)
echo Python найден: %PY_LAUNCHER%
echo.

echo [2/6] Создание виртуального окружения .venv...
if not exist ".venv\Scripts\python.exe" (
    %PY_LAUNCHER% -m venv .venv
    if errorlevel 1 (
        echo ОШИБКА: Не удалось создать виртуальное окружение .venv
        pause
        exit /b 1
    )
) else (
    echo Виртуальное окружение уже существует.
)
echo.

echo [3/6] Установка зависимостей...
if not exist ".venv\Scripts\pip.exe" (
    echo ОШИБКА: Не найден .venv\Scripts\pip.exe
    pause
    exit /b 1
)
.venv\Scripts\pip install -r requirements.txt
if errorlevel 1 (
    echo ОШИБКА: Не удалось установить зависимости.
    pause
    exit /b 1
)
echo.

echo [4/6] Подготовка файла .env...
if not exist ".env" (
    if not exist ".env.example" (
        echo ОШИБКА: Не найден файл .env.example
        pause
        exit /b 1
    )
    copy .env.example .env >nul
    if errorlevel 1 (
        echo ОШИБКА: Не удалось создать .env из .env.example
        pause
        exit /b 1
    )
    echo Создан файл .env — проверьте настройки подключения к БД
    notepad .env
) else (
    echo Файл .env уже существует.
)
echo.

echo [5/6] Проверка подключения к БД...
.venv\Scripts\python -c "from config.db import check_connection; check_connection()"
if errorlevel 1 (
    echo ОШИБКА: Не удалось подключиться к базе данных.
    echo Проверьте что PostgreSQL запущен и настройки в файле .env верны.
    pause
    exit /b 1
)
echo.

echo [6/6] Завершение...
echo === Установка завершена ===
echo Теперь запустите: start_web_app.cmd
pause
exit /b 0
