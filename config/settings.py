import os
from config.paths import BASE_DIR


class Settings:

    def __init__(self, input_file=None, selected_table=None, output_folder=None):
        """
        Инициализация настроек.
        
        Args:
            input_file: путь к загруженному файлу (опционально)
            selected_table: имя выбранной таблицы (опционально)
            output_folder: папка для вывода результатов (опционально)
        """
        self.input_file = input_file
        self.selected_table = selected_table
        
        # Определяем имя проекта
        if selected_table:
            self.project_name = selected_table
        elif input_file:
            self.project_name = os.path.splitext(os.path.basename(input_file))[0]
        else:
            self.project_name = "default_project"
        
        # Если output_folder не указан, создаем стандартный путь
        if output_folder:
            self.output_folder = output_folder
        else:
            # Папка results внутри BASE_DIR
            results_dir = os.path.join(BASE_DIR, "results")
            os.makedirs(results_dir, exist_ok=True)
            
            # Папка проекта внутри results
            self.output_folder = os.path.join(results_dir, f"folder_{self.project_name}")
            os.makedirs(self.output_folder, exist_ok=True)
    
    def __repr__(self):
        return f"Settings(project_name={self.project_name}, selected_table={self.selected_table}, output_folder={self.output_folder})"