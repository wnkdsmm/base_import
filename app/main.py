from fastapi import FastAPI, Request, UploadFile, File, Body, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import os
import shutil
import sys
import io
from pathlib import Path
from datetime import datetime

# РРјРїРѕСЂС‚ pipeline Рё С€Р°РіРѕРІ
from config.settings import Settings
from pipeline import Pipeline
from steps.fires_feature_profiling import FiresFeatureProfilingStep
from steps.create_clean_table import CreateCleanTableStep
from steps.import_data import ImportDataStep
from steps.create_fire_map import CreateFireMapStep
from steps.keep_important_columns import get_column_matcher
from config.db import engine

# Р›РѕРіРёСЂРѕРІР°РЅРёРµ Рё DB Views
from app.log_manager import add_log, get_logs, clear_logs
from app.db_views import get_all_tables, get_table_columns, get_table_data, get_table_preview
from app.statistics import build_dashboard_context, get_dashboard_data

# --------------------- РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ ---------------------

app = FastAPI(title="рџ”Ґ Fire Data Pipeline")

# РЎС‚Р°С‚РёС‡РµСЃРєРёРµ С„Р°Р№Р»С‹ Рё С€Р°Р±Р»РѕРЅС‹
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# РџР°РїРєР° РґР»СЏ Р·Р°РіСЂСѓР·РѕРє
UPLOAD_FOLDER = "uploads"
CURRENT_UPLOAD_FILE = os.path.join(UPLOAD_FOLDER, "current_upload.xlsx")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Р“Р»РѕР±Р°Р»СЊРЅС‹Рµ РїРµСЂРµРјРµРЅРЅС‹Рµ
uploaded_file_path = None
uploaded_files_history = {}

# --------------------- Р­РЅРґРїРѕРёРЅС‚С‹ ---------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request, table_name: str = "all", year: str = "all", group_column: str = ""):
    try:
        dashboard = build_dashboard_context(table_name=table_name, year=year, group_column=group_column)
    except Exception as exc:
        dashboard = {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": [{"value": "all", "label": "Все таблицы"}],
                "years": [{"value": "all", "label": "Все годы"}],
                "group_columns": [],
            },
            "initial_data": get_dashboard_data(),
            "errors": [str(exc)],
            "has_data": False,
        }

    return templates.TemplateResponse("index.html", {"request": request, "dashboard": dashboard})


@app.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    return get_dashboard_data(table_name=table_name, year=year, group_column=group_column)



@app.get("/api/column-search")
def column_search_endpoint(table_name: str = "", query: str = ""):
    query_text = query.strip()
    if not table_name:
        return {"table_name": "", "query": query_text, "count": 0, "columns": [], "message": "Выберите таблицу для поиска колонок."}
    if not query_text:
        return {"table_name": table_name, "query": "", "count": 0, "columns": [], "message": "Введите слова через пробел."}

    try:
        columns = get_table_columns(table_name)
    except Exception as exc:
        return {"table_name": table_name, "query": query_text, "count": 0, "columns": [], "message": str(exc)}

    try:
        matcher = get_column_matcher()
        matches = matcher.find_columns_by_query(columns, query_text)
    except Exception as exc:
        return {"table_name": table_name, "query": query_text, "count": 0, "columns": [], "message": f"Natasha-поиск не сработал: {exc}"}

    preview_columns = []
    preview_rows = []
    if matches:
        try:
            preview_columns, preview_rows = get_table_preview(
                table_name,
                [item["name"] for item in matches],
                limit=100,
            )
        except Exception as exc:
            return {
                "table_name": table_name,
                "query": query_text,
                "count": len(matches),
                "columns": matches,
                "message": f"Совпадения найдены, но превью таблицы не удалось загрузить: {exc}",
                "preview_columns": [],
                "preview_rows": [],
            }

    message = "Совпадения найдены." if matches else "Совпадений по этому запросу не найдено."
    return {
        "table_name": table_name,
        "query": query_text,
        "count": len(matches),
        "columns": matches,
        "message": message,
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
    }

def _get_fire_map_table_options():
    try:
        options = get_dashboard_data().get("filters", {}).get("available_tables", [])
    except Exception:
        options = []

    table_options = []
    seen = set()
    for option in options:
        value = option.get("value")
        if not value or value == "all" or value in seen:
            continue
        seen.add(value)
        table_options.append({"value": value, "label": option.get("label", value)})

    if table_options:
        return table_options

    fallback_tables = []
    for table_name in get_all_tables():
        if table_name.startswith(("clean_", "final_", "tmp_", "pg_", "sql_")):
            continue
        fallback_tables.append({"value": table_name, "label": table_name})
    return fallback_tables


def _render_fire_map_error_html(message: str) -> str:
    return f"""<!DOCTYPE html>
<html lang=\"ru\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>Карта пожаров</title>
    <style>
        body {{
            margin: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: Bahnschrift, 'Segoe UI', sans-serif;
            background: linear-gradient(180deg, #f8f3ea 0%, #f1e7d9 100%);
            color: #2b2119;
        }}
        .map-error {{
            max-width: 680px;
            padding: 28px 30px;
            border-radius: 24px;
            background: rgba(255, 252, 247, 0.94);
            border: 1px solid rgba(94, 73, 49, 0.12);
            box-shadow: 0 18px 48px rgba(73, 49, 21, 0.12);
        }}
        h1 {{ margin: 0 0 12px; font-size: 28px; }}
        p {{ margin: 0; line-height: 1.6; color: #6c655d; }}
    </style>
</head>
<body>
    <div class=\"map-error\">
        <h1>Карта пока недоступна</h1>
        <p>{message}</p>
    </div>
</body>
</html>"""


@app.get("/column-search", response_class=HTMLResponse)
def column_search_page(request: Request, table_name: str = "", query: str = ""):
    try:
        table_options = [
            option for option in get_dashboard_data().get("filters", {}).get("available_tables", [])
            if option.get("value") and option.get("value") != "all"
        ]
    except Exception:
        table_options = []

    if not table_options:
        table_options = [
            {"value": name, "label": name}
            for name in get_all_tables()
            if not name.startswith(("clean_", "final_", "tmp_", "pg_", "sql_"))
        ]

    available_values = {option["value"] for option in table_options}
    selected_table = table_name if table_name in available_values else (table_options[0]["value"] if table_options else "")

    return templates.TemplateResponse(
        "column_search.html",
        {
            "request": request,
            "table_options": table_options,
            "selected_table": selected_table,
            "initial_query": query,
        },
    )

@app.get("/fire-map", response_class=HTMLResponse)
def fire_map_page(request: Request, table_name: str = ""):
    table_options = _get_fire_map_table_options()
    available_values = {option["value"] for option in table_options}
    selected_table = table_name if table_name in available_values else (table_options[0]["value"] if table_options else "")

    return templates.TemplateResponse(
        "fire_map.html",
        {
            "request": request,
            "table_options": table_options,
            "selected_table": selected_table,
            "tables_count": len(table_options),
        },
    )


@app.get("/fire-map/embed", response_class=HTMLResponse)
def fire_map_embed(table_name: str = ""):
    table_options = _get_fire_map_table_options()
    available_values = {option["value"] for option in table_options}
    if not table_name or table_name not in available_values:
        return HTMLResponse(_render_fire_map_error_html("Выберите существующую таблицу для построения карты."), status_code=400)

    try:
        settings = Settings(
            input_file=None,
            selected_table=table_name,
            output_folder=os.path.join("results", f"folder_{table_name}"),
        )
        output_path = CreateFireMapStep().run(settings, table_name=table_name)
        if not output_path:
            return HTMLResponse(_render_fire_map_error_html("Для выбранной таблицы не удалось собрать карту. Проверьте координаты Широта и Долгота."), status_code=422)

        map_html = Path(output_path).read_text(encoding="utf-8")
        return HTMLResponse(map_html)
    except Exception as exc:
        return HTMLResponse(_render_fire_map_error_html(str(exc)), status_code=500)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    global uploaded_file_path, uploaded_files_history  # РЅСѓР¶РЅРѕ РїРµСЂРІС‹Рј РґРµР»РѕРј

    original_filename = file.filename
    file_path = os.path.join(UPLOAD_FOLDER, original_filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    uploaded_file_path = file_path

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    uploaded_files_history[file_path] = {
        "original_name": original_filename,
        "upload_time": timestamp,
        "path": file_path
    }

    add_log(f"рџ“¤ Р¤Р°Р№Р» Р·Р°РіСЂСѓР¶РµРЅ: {original_filename} (РїРµСЂРµР·Р°РїРёСЃР°РЅ, РµСЃР»Рё СЃСѓС‰РµСЃС‚РІРѕРІР°Р»)")

    return {
        "status": "uploaded",
        "filename": original_filename,
        "path": file_path
    }

@app.get("/logs")
def logs():
    return {"logs": get_logs()}


@app.post("/clear_logs")
def clear_logs_endpoint():
    clear_logs()
    return {"status": "cleared"}


@app.get("/health")
def health_check():
    return {"status": "healthy", "uploaded_file": uploaded_file_path is not None}


@app.get("/tables", response_class=HTMLResponse)
async def list_tables(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse("tables.html", {"request": request, "tables": tables})


@app.get("/tables/{table_name}", response_class=HTMLResponse)
async def view_table(request: Request, table_name: str):
    try:
        columns, rows = get_table_data(table_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=404, detail="Table not found")
    
    return templates.TemplateResponse(
        "table_view.html",
        {"request": request, "table_name": table_name, "columns": columns, "rows": rows}
    )


@app.get("/select_table", response_class=HTMLResponse)
def select_table(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse("select_table.html", {"request": request, "tables": tables})


@app.post("/run_profiling")
def run_profiling_endpoint(payload: dict = Body(...)):
    table_name = payload.get("table")
    if not table_name:
        return {"status": "вќЊ No table selected"}

    clear_logs()
    add_log(f"рџљЂ Starting pipeline for table: {table_name}")

    old_stdout = sys.stdout
    buffer = io.StringIO()
    sys.stdout = buffer

    try:
        settings = Settings(
            input_file=None, 
            selected_table=table_name,
            output_folder=os.path.join("results", f"folder_{table_name}")
        )
        
        add_log(f"рџ“Љ Processing table: {table_name}")
        add_log(f"рџ“‚ Output folder: {settings.output_folder}")
        
        add_log("рџ”Ќ Step 1: Running feature profiling...")
        profiling_step = FiresFeatureProfilingStep(settings)
        profiling_step.run(settings)
        
        add_log("рџ§№ Step 2: Creating clean table...")
        clean_step = CreateCleanTableStep()
        clean_step.run(settings)
        
        add_log(f"вњ… Pipeline СѓСЃРїРµС€РЅРѕ РІС‹РїРѕР»РЅРµРЅ РґР»СЏ С‚Р°Р±Р»РёС†С‹ {table_name}")
        
        clean_table_name = f"clean_{table_name}"
        profile_csv = os.path.join(settings.output_folder, f"{table_name}_profile.csv")
        clean_xlsx = os.path.join(settings.output_folder, f"{clean_table_name}.xlsx")
        
        add_log(f"рџ“Љ РџСЂРѕС„РёР»СЊ СЃРѕС…СЂР°РЅРµРЅ: {profile_csv}")
        add_log(f"рџ“Љ Р§РёСЃС‚Р°СЏ С‚Р°Р±Р»РёС†Р°: {clean_table_name}")
        add_log(f"рџ“Љ Excel С„Р°Р№Р»: {clean_xlsx}")
        
    except FileNotFoundError as e:
        error_msg = f"вќЊ File not found: {str(e)}"
        add_log(error_msg)
        return {"status": error_msg}
    except ValueError as e:
        error_msg = f"вќЊ Value error: {str(e)}"
        add_log(error_msg)
        return {"status": error_msg}
    except Exception as e:
        error_msg = f"вќЊ Error: {str(e)}"
        add_log(error_msg)
        return {"status": error_msg}
    finally:
        sys.stdout = old_stdout

    logs = buffer.getvalue().split("\n")
    for l in logs:
        if l.strip():
            add_log(l)

    return {
        "status": f"вњ… Profiling and cleaning done for {table_name}",
        "output_folder": settings.output_folder,
        "clean_table": f"clean_{table_name}"
    }


@app.post("/import_data")
def import_data_endpoint(output_folder: str = Form(None)):
    global uploaded_file_path  # вњ… РїРµСЂРІРѕР№ СЃС‚СЂРѕРєРѕР№ РїРѕСЃР»Рµ def

    if not uploaded_file_path or not os.path.exists(uploaded_file_path):
        return {"status": "вќЊ No file uploaded", "rows": 0, "columns": 0}

    clear_logs()
    add_log(f"рџљЂ Starting ImportDataStep for {uploaded_file_path}")

    settings = Settings(
        input_file=uploaded_file_path,
        output_folder=output_folder if output_folder else None
    )

    add_log(f"рџ“ќ Project name: {settings.project_name}")
    add_log(f"рџ“Ѓ Output folder: {settings.output_folder}")

    step = ImportDataStep()

    try:
        step.run(settings)
        add_log(f"вњ… Import completed: {uploaded_file_path}")

        # РџРѕСЃР»Рµ СѓСЃРїРµС€РЅРѕРіРѕ РёРјРїРѕСЂС‚Р° РѕР±РЅСѓР»СЏРµРј РїСѓС‚СЊ
        uploaded_file_path = None

        if step.data is not None:
            return {
                "status": "вњ… Import successful",
                "rows": step.data.shape[0],
                "columns": step.data.shape[1],
                "project_name": settings.project_name,
                "output_folder": settings.output_folder
            }
        else:
            return {
                "status": "вљ пёЏ Import completed but no data available",
                "rows": 0,
                "columns": 0,
                "project_name": settings.project_name
            }

    except Exception as e:
        error_msg = f"вќЊ Import failed: {str(e)}"
        add_log(error_msg)
        return {"status": error_msg, "rows": 0, "columns": 0}













