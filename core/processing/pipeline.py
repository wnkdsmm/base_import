# pipeline.py
import logging
import time


logger = logging.getLogger(__name__)


class PipelineStep:
    """Базовый класс для всех шагов пайплайна."""

    def __init__(self, name: str):
        self.name = name

    def run(self, settings):
        """
        Метод для реализации логики шага в наследниках.
        Каждый шаг получает объект Settings с путями и именем проекта.
        """
        raise NotImplementedError


class Pipeline:
    """Менеджер пайплайна, запускает шаги последовательно."""

    def __init__(self, settings):
        self.settings = settings
        self.steps = []

    def add_step(self, step: PipelineStep):
        self.steps.append(step)

    def run(self) -> dict:
        logger.info("\nЗапуск конвейера: %s\n", self.settings.project_name)
        step_results = []

        for step in self.steps:
            logger.info("\nШаг: %s", step.name)
            start_time = time.time()
            try:
                # Передаем весь объект settings шагу
                result = step.run(self.settings)
            except Exception as exc:
                logger.exception("Ошибка на шаге %s", step.name)
                return {
                    "success": False,
                    "project_name": self.settings.project_name,
                    "failed_step": step.name,
                    "error": str(exc),
                    "step_results": step_results,
                }

            elapsed = time.time() - start_time
            logger.info("Шаг завершён: %s (%.2f с)", step.name, elapsed)
            step_results.append(
                {
                    "step": step.name,
                    "result": result,
                    "elapsed_seconds": elapsed,
                }
            )

        logger.info("\nКонвейер завершён: %s", self.settings.project_name)
        return {
            "success": True,
            "project_name": self.settings.project_name,
            "step_results": step_results,
        }
