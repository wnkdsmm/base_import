# pipeline.py
import time
import traceback


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

    def run(self):
        print(f"\nЗапуск конвейера: {self.settings.project_name}\n")
        for step in self.steps:
            print(f"\nШаг: {step.name}")
            start_time = time.time()
            try:
                # Передаем весь объект settings шагу
                step.run(self.settings)
            except Exception:
                print(f"Ошибка на шаге {step.name}")
                traceback.print_exc()
                break
            elapsed = time.time() - start_time
            print(f"Шаг завершён: {step.name} ({elapsed:.2f} с)")

        print(f"\nКонвейер завершён: {self.settings.project_name}")