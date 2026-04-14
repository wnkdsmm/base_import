# pipeline.py
import logging
import time


logger = logging.getLogger(__name__)


class PipelineStep:
    """Р‘Р°Р·РѕРІС‹Р№ РєР»Р°СЃСЃ РґР»СЏ РІСЃРµС… С€Р°РіРѕРІ РїР°Р№РїР»Р°Р№РЅР°."""

    def __init__(self, name: str):
        self.name = name

    def run(self, settings):
        """
        РњРµС‚РѕРґ РґР»СЏ СЂРµР°Р»РёР·Р°С†РёРё Р»РѕРіРёРєРё С€Р°РіР° РІ РЅР°СЃР»РµРґРЅРёРєР°С….
        РљР°Р¶РґС‹Р№ С€Р°Рі РїРѕР»СѓС‡Р°РµС‚ РѕР±СЉРµРєС‚ Settings СЃ РїСѓС‚СЏРјРё Рё РёРјРµРЅРµРј РїСЂРѕРµРєС‚Р°.
        """
        raise NotImplementedError


class Pipeline:
    """РњРµРЅРµРґР¶РµСЂ РїР°Р№РїР»Р°Р№РЅР°, Р·Р°РїСѓСЃРєР°РµС‚ С€Р°РіРё РїРѕСЃР»РµРґРѕРІР°С‚РµР»СЊРЅРѕ."""

    def __init__(self, settings):
        self.settings = settings
        self.steps = []

    def add_step(self, step: PipelineStep):
        self.steps.append(step)

    def run(self):
        logger.info("\nР—Р°РїСѓСЃРє РєРѕРЅРІРµР№РµСЂР°: %s\n", self.settings.project_name)
        for step in self.steps:
            logger.info("\nРЁР°Рі: %s", step.name)
            start_time = time.time()
            try:
                # РџРµСЂРµРґР°РµРј РІРµСЃСЊ РѕР±СЉРµРєС‚ settings С€Р°РіСѓ
                step.run(self.settings)
            except Exception:
                logger.exception("РћС€РёР±РєР° РЅР° С€Р°РіРµ %s", step.name)
                break
            elapsed = time.time() - start_time
            logger.info("РЁР°Рі Р·Р°РІРµСЂС€С‘РЅ: %s (%.2f СЃ)", step.name, elapsed)

        logger.info("\nРљРѕРЅРІРµР№РµСЂ Р·Р°РІРµСЂС€С‘РЅ: %s", self.settings.project_name)
