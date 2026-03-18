from pathlib import Path

import pandas as pd
from sqlalchemy import inspect

from config.db import engine
from core.mapping.fire_map_generator import Config, MapCreator


class CreateFireMapStep:
    name = "CreateFireMapStep"

    def run(self, settings, table_name=None):
        print("Creating fire map")

        inspector = inspect(engine)

        if table_name is None:
            table_name = settings.project_name

        if table_name not in inspector.get_table_names():
            print(f"Table '{table_name}' was not found in the database")
            print(f"Available tables: {inspector.get_table_names()}")
            return None

        try:
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            df = pd.read_sql(query, engine)

            if df.empty:
                print(f"Table '{table_name}' is empty")
                return None

            print(f"Loaded table '{table_name}' ({len(df)} rows)")
        except Exception as exc:
            print(f"Failed to load table '{table_name}': {exc}")
            return None

        tables_data = {table_name: df}

        config = Config(output_dir=Path(settings.output_folder))
        creator = MapCreator(config)
        output = creator.create_map(tables_data)

        if output:
            print(f"Map created: {output}")
            print(f"Total points on map: {len(df)}")
        else:
            print(f"Failed to create map for table '{table_name}'")

        return output
