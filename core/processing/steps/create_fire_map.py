from pathlib import Path

from app.db_metadata import get_table_names_cached
from core.mapping.fire_map_generator import Config, MapCreator
from core.processing.steps.fire_map_loader import load_fire_map_source


class CreateFireMapStep:
    name = "CreateFireMapStep"

    def run(self, settings, table_name=None):
        print("Creating fire map")

        if table_name is None:
            table_name = settings.project_name

        config = Config(output_dir=Path(settings.output_folder))

        try:
            source = load_fire_map_source(table_name, config)
            df = source["dataframe"]

            if df.empty:
                print(f"Table '{table_name}' is empty")
                return None

            print(
                f"Loaded table '{table_name}' ({len(df)} rows, {len(source['selected_columns'])} selected columns, limit {source['limit']})"
            )
        except ValueError as exc:
            print(str(exc))
            if "не найдена" in str(exc):
                print(f"Available tables: {get_table_names_cached()}")
            return None
        except Exception as exc:
            print(f"Failed to load table '{table_name}': {exc}")
            return None

        tables_data = {table_name: df}

        creator = MapCreator(config)
        output = creator.create_map(tables_data)

        if output:
            print(f"Map created: {output}")
            print(f"Total points on map: {len(df)}")
            analysis_json = Path(settings.output_folder) / "fires_map_analysis.json"
            analysis_md = Path(settings.output_folder) / "fires_map_analysis.md"
            if analysis_json.exists():
                print(f"Spatial analytics JSON: {analysis_json}")
            if analysis_md.exists():
                print(f"Spatial analytics Markdown: {analysis_md}")
        else:
            print(f"Failed to create map for table '{table_name}'")

        return output