from sqlalchemy import text, inspect
from config.db import engine

def _quote_identifier(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'

def get_all_tables():
    """РџРѕР»СѓС‡РёС‚СЊ СЃРїРёСЃРѕРє РІСЃРµС… С‚Р°Р±Р»РёС† РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С…"""
    inspector = inspect(engine)
    return inspector.get_table_names()


def get_table_columns(table_name):
    """РџРѕР»СѓС‡РёС‚СЊ СЃРїРёСЃРѕРє РєРѕР»РѕРЅРѕРє РґР»СЏ С‚Р°Р±Р»РёС†С‹."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist")

    return [col['name'] for col in inspector.get_columns(table_name)]

def get_table_preview(table_name, selected_columns, limit=100):
    """РџСЂРµРІСЊСЋ РІС‹Р±СЂР°РЅРЅС‹С… РєРѕР»РѕРЅРѕРє РёР· С‚Р°Р±Р»РёС†С‹."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    requested_columns = [str(column) for column in (selected_columns or []) if column]
    if not requested_columns:
        return [], []

    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist")

    table_columns = [col['name'] for col in inspector.get_columns(table_name)]
    available_columns = [column for column in requested_columns if column in table_columns]
    if not available_columns:
        return [], []

    quoted_columns = ', '.join(_quote_identifier(column) for column in available_columns)
    query = text(f'SELECT {quoted_columns} FROM {_quote_identifier(table_name)} LIMIT :limit')

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [list(row) for row in result]

    return available_columns, rows
def get_table_data(table_name, limit=100):
    """РџРѕР»СѓС‡РёС‚СЊ РґР°РЅРЅС‹Рµ РёР· С‚Р°Р±Р»РёС†С‹ СЃ РѕРіСЂР°РЅРёС‡РµРЅРёРµРј РїРѕ СЃС‚СЂРѕРєР°Рј"""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")
    
    try:
        with engine.connect() as conn:
            # РџСЂРѕРІРµСЂСЏРµРј СЃСѓС‰РµСЃС‚РІРѕРІР°РЅРёРµ С‚Р°Р±Р»РёС†С‹
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # РџРѕР»СѓС‡Р°РµРј РєРѕР»РѕРЅРєРё
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            # РџРѕР»СѓС‡Р°РµРј РґР°РЅРЅС‹Рµ
            query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
            result = conn.execute(query, {"limit": limit})
            rows = [list(row) for row in result]
            
            return columns, rows
    except Exception as e:
        raise Exception(f"Error accessing table {table_name}: {str(e)}")





