# Fire Data Base Import

Минимальная инфраструктурная памятка для локального запуска, benchmark и служебных проверок после cleanup/refactor.

## Почему здесь `requirements.txt`, а не `pyproject.toml`

Проект сейчас запускается как набор FastAPI-модулей, служебных скриптов и legacy desktop entrypoint'ов без необходимости в packaging/refactor.
Самый простой и совместимый шаг здесь — обычный `requirements.txt`:

- не меняет текущий сценарий запуска;
- работает и с `py -m pip`, и с `.venv\Scripts\pip`;
- не навязывает Poetry/uv/pdm;
- подходит и для FastAPI, и для benchmark/pipeline-скриптов.

## Зависимости

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Если используется уже готовый интерпретатор, можно поставить зависимости и в него:

```powershell
python -m pip install -r requirements.txt
```

Ниже команды с `.\.venv\Scripts\python.exe` предполагают, что виртуальное окружение создано шагами выше.
Если используется другой интерпретатор, замените этот префикс на свой рабочий вариант: `APP_PYTHON`, `py` или `python`.

## База данных

По умолчанию проект ожидает PostgreSQL и читает `DATABASE_URL` из окружения.
Если переменная не задана, используется fallback из [config/db.py](/F:/filesFires/base_import/config/db.py).

Пример:

```powershell
$env:DATABASE_URL = "postgresql://postgres:1234@localhost/fires"
```

## Запуск FastAPI

Основной entrypoint:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Windows-обёртки тоже сохранены:

- [start_web_app.cmd](/F:/filesFires/base_import/start_web_app.cmd)
- [start_web_app.vbs](/F:/filesFires/base_import/start_web_app.vbs)

Они теперь ищут интерпретатор в таком порядке:

1. `APP_PYTHON`
2. `.venv\Scripts\python.exe`
3. `uvicorn` в `PATH`
4. `py`
5. `python`
6. стандартные user install dirs под `%LocalAppData%\Programs\Python\...` и `%LocalAppData%\Python\...`

Если нужно жёстко указать интерпретатор:

```powershell
$env:APP_PYTHON = "C:\\path\\to\\python.exe"
.\start_web_app.cmd
```

## Legacy desktop pipeline

Старый интерактивный pipeline entrypoint сохранён:

```powershell
.\.venv\Scripts\python.exe main.py
```

Он открывает file picker через `tkinter` и запускает import/profiling/clean-table pipeline.

## Forecasting Materialized Views

Подготовка materialized views для forecasting/ML:

```powershell
.\.venv\Scripts\python.exe -m scripts.prepare_forecasting_aggregates
.\.venv\Scripts\python.exe -m scripts.prepare_forecasting_aggregates --table clean_ekup_Yemelyanovo_2025
.\.venv\Scripts\python.exe -m scripts.prepare_forecasting_aggregates --table clean_ekup_Yemelyanovo_2025 --no-refresh
```

Скрипт работает только там, где текущая БД действительно PostgreSQL.

## Benchmark

Cold benchmark:

```powershell
.\.venv\Scripts\python.exe -m scripts.benchmark_analytics_perf --rows 50000
.\.venv\Scripts\python.exe -m scripts.benchmark_analytics_perf --rows 20000 --keep-table
```

Что важно:

- benchmark требует PostgreSQL;
- скрипт создаёт временные таблицы `benchmark_fire_perf_*` в БД, а не файлы в репозитории;
- временные benchmark-таблицы удаляются автоматически, если не передан `--keep-table`;
- после недавнего скрытия `benchmark_*` из обычного списка таблиц UI/dashboard-path на synthetic table ограничен, поэтому для real dashboard read-path полезно дополнять benchmark perf-логами на настоящих таблицах.

## Generated Artifacts

Generated/local-only артефактами считаются:

- Python cache: `__pycache__/`, `*.py[cod]`
- tool caches: `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.coverage`, `htmlcov/`
- packaging leftovers: `*.egg-info/`
- runtime logs: `*.log`, `results/`, `logs/`
- pipeline outputs и загрузки: `data/results/`, `data/uploads/`
- локальные temp files: `tmp_*.txt`
- локальные env dirs: `.venv/`, `venv/`, `env/`, `ENV/`

Если нужны ручные demo/sample upload-файлы, держите их вне generated-путей.
В этом репозитории для этого теперь используется `sample_data/uploads/`.

Для быстрой проверки рабочего дерева:

```powershell
.\.venv\Scripts\python.exe -m scripts.check_generated_artifacts
```

Чтобы отдельно увидеть исторически уже отслеживаемые generated-файлы:

```powershell
.\.venv\Scripts\python.exe -m scripts.check_generated_artifacts --tracked
```

Важно: generated outputs и runtime uploads больше не должны попадать в git.
Если нужен воспроизводимый ручной сценарий, используйте sample-файлы из `sample_data/uploads/`, а результаты генерируйте локально.
