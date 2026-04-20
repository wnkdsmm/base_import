from __future__ import annotations

import logging
import time
from typing import Any


logger = logging.getLogger(__name__)


class PipelineStep:
    """Базовый класс для всех шагов пайплайна."""

    def __init__(self, name: str):
        self.name = name

    def run(self, settings: Any, **kwargs: object) -> dict[str, object] | None:
        """
        Логика шага. settings — объект конфигурации.
        Подклассы могут принимать дополнительные kwargs.
        """
        raise NotImplementedError


class Pipeline:
    """Менеджер пайплайна, запускает шаги последовательно."""

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.steps = []
        self._step_kwargs: dict[str, dict[str, object]] = {}

    def add_step(self, step: PipelineStep) -> None:
        self.steps.append(step)

    def set_step_kwargs(self, step_name: str, **kwargs: object) -> None:
        self._step_kwargs[step_name] = kwargs

    def run(self) -> dict[str, object]:
        logger.info("\nЗапуск конвейера: %s\n", self.settings.project_name)
        step_results = []

        for step in self.steps:
            logger.info("\nШаг: %s", step.name)
            start_time = time.time()
            try:
                result = step.run(self.settings, **self._step_kwargs.get(step.name, {}))
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
