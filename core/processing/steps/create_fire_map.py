import logging
from pathlib import Path

from app.db_metadata import get_table_names_cached
from core.mapping.config import Config
from core.mapping.creator import MapCreator
from core.processing.steps.fire_map_loader import load_fire_map_source


logger = logging.getLogger(__name__)


class CreateFireMapStep:
    name = "CreateFireMapStep"

    def run(self, settings, table_name=None):
        logger.info("Creating fire map")

        if table_name is None:
            table_name = settings.project_name

        config = Config(output_dir=Path(settings.output_folder))

        try:
            source = load_fire_map_source(table_name, config)
            df = source["dataframe"]

            if df.empty:
                logger.warning("Table '%s' is empty", table_name)
                return None

            logger.info(
                "Loaded table '%s' (%s rows, %s selected columns, limit %s)",
                table_name,
                len(df),
                len(source["selected_columns"]),
                source["limit"],
            )
        except ValueError as exc:
            logger.warning("%s", exc)
            if "РЅРµ РЅР°Р№РґРµРЅР°" in str(exc):
                logger.info("Available tables: %s", get_table_names_cached())
            return None
        except Exception:
            logger.exception("Failed to load table '%s'", table_name)
            return None

        tables_data = {table_name: df}

        creator = MapCreator(config)
        output = creator.create_map(tables_data)

        if output:
            logger.info("Map created: %s", output)
            logger.info("Total points on map: %s", len(df))
            analysis_json = Path(settings.output_folder) / "fires_map_analysis.json"
            analysis_md = Path(settings.output_folder) / "fires_map_analysis.md"
            if analysis_json.exists():
                logger.info("Spatial analytics JSON: %s", analysis_json)
            if analysis_md.exists():
                logger.info("Spatial analytics Markdown: %s", analysis_md)
        else:
            logger.warning("Failed to create map for table '%s'", table_name)

        return output
