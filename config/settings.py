import os
from pathlib import Path

from config.paths import get_result_folder


class Settings:

    def __init__(self, input_file=None, selected_table=None, output_folder=None):
        """
        РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ РЅР°СЃС‚СЂРѕРµРє.
        
        Args:
            input_file: РїСѓС‚СЊ Рє Р·Р°РіСЂСѓР¶РµРЅРЅРѕРјСѓ С„Р°Р№Р»Сѓ (РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ)
            selected_table: РёРјСЏ РІС‹Р±СЂР°РЅРЅРѕР№ С‚Р°Р±Р»РёС†С‹ (РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ)
            output_folder: РїР°РїРєР° РґР»СЏ РІС‹РІРѕРґР° СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ (РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ)
        """
        self.input_file = input_file
        self.selected_table = selected_table
        
        if selected_table:
            self.project_name = selected_table
        elif input_file:
            self.project_name = os.path.splitext(os.path.basename(input_file))[0]
        else:
            self.project_name = "default_project"
        
        if output_folder:
            output_path = Path(output_folder)
            output_path.mkdir(parents=True, exist_ok=True)
            self.output_folder = str(output_path)
        else:
            self.output_folder = str(get_result_folder(self.project_name))
    
    def __repr__(self):
        return f"Settings(project_name={self.project_name}, selected_table={self.selected_table}, output_folder={self.output_folder})"
