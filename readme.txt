Fire Data Base Import - быстрый перенос и запуск на другом ПК

1) Что нужно на новом устройстве
- Windows 10/11
- Python 3.11+ (обязательно добавить в PATH)
- PostgreSQL (доступ к базе и корректный DATABASE_URL)

2) Быстрая установка (онлайн)
Откройте PowerShell в папке проекта и выполните:

py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

3) Настройка .env
- Скопируйте .env.example в .env
- Проверьте DATABASE_URL, APP_HOST, APP_PORT

Пример DATABASE_URL:
postgresql://postgres:1234@localhost:5432/fires

4) Запуск веб-приложения
Вариант A (рекомендуется):
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

Вариант B (двойной клик):
start_web_app.vbs

После запуска откройте:
http://127.0.0.1:8000/

5) Офлайн-установка (без интернета)
На ПК с интернетом:
download_packages.bat

Перенесите проект вместе с папкой wheels на целевой ПК и запустите:
install_offline.bat

6) Проверка работоспособности
- Приложение открывается в браузере
- В логах нет ошибок подключения к БД
- API отвечает по http://127.0.0.1:8000/docs

7) Что уже очищено для переноса
- Удалены служебные файлы Codex/Claude (.claude)
- Удалены локальные кэши и runtime-логи (.pytest_cache, __pycache__, logs)

Примечание:
Файл README.md можно оставить для полной документации, а этот readme.txt использовать как короткую инструкцию для переноса.
