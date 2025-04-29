import os, json, shutil
from zipfile import ZipFile
from pathlib import Path
from constants import Constants
from common import *

SOURCE_EXTENSIONS = [Constants.PYTHON_EXTENSION, Constants.SQL_EXTENSION, Constants.SCALA_EXTENSION, Constants.R_EXTENSION]

class DbcToJupyter:
    # Extract the Databricks notebook
    def __extract_dbc(self, file_absoulute_path: str) -> str:
        with ZipFile(file_absoulute_path, 'r') as dbc:
            global TEMP_FOLDER
            TEMP_FOLDER = f"temp_{Path(file_absoulute_path).name}"
            temp_path = os.path.join(os.path.dirname(file_absoulute_path), TEMP_FOLDER)
            dbc.extractall(temp_path)

            manifest_path = os.path.join(temp_path, "manifest.mf")
            os.remove(manifest_path)
            return temp_path

    # Read the Databricks notebook
    def __read_notebook(self, json_absolute_path: str) -> json:
        with open(json_absolute_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data

    # Get the cells from the Databricks notebook    
    def __get_cells(self, data: json) -> list:
        commands = sorted(data["commands"], key=lambda x: x["position"])
        cells = []

        for cell in commands:
            cells.append(cell["command"])

        return cells
    
    # Main function
    def main(self, input_folder: str, dbc_absoulute_path: str, output_folder: str) -> None:
        print(f"START Converting the Dbc notebooks: '{dbc_absoulute_path}'")
        notebooks_path = self.__extract_dbc(dbc_absoulute_path)

        for path, _, files in os.walk(notebooks_path):
            for file in files:
                json_absolute_path = os.path.join(path, file)
                json_relative_path = os.path.relpath(json_absolute_path, input_folder).replace(f"{TEMP_FOLDER}{os.sep}", "")

                if file.lower().endswith(tuple(SOURCE_EXTENSIONS)):
                    print(f"   START Converting the notebook: '{json_absolute_path}'")
                    json = self.__read_notebook(json_absolute_path)
                    cells = self.__get_cells(json)
                    notebook = create_notebook(cells)
                    save_notebook(output_folder, json_relative_path, notebook)
                    print(f"   FINISH Converting the notebook: '{json_absolute_path}'")
                    
                else:
                    print(f"File not supported: {json_absolute_path}")
                    continue

        shutil.rmtree(notebooks_path)
        print(f"FINISH Converting the Dbc notebooks: '{dbc_absoulute_path}'")