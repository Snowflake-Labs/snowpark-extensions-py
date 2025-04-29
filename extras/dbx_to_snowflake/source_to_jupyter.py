from pathlib import Path
from constants import Constants
from common import create_notebook, save_notebook

class SourceToJupyter:
    # Read the Databricks notebook
    def __read_notebook(self, file_absolute_path: str) -> str:
        with open(file_absolute_path, "r", encoding="utf-8") as file:
            content = file.read()
            return content


    # Split the source notebook into cells
    def __get_cells_by_extension(self, content: str, extension: str) -> list[str]:
        header = None
        comment = None
        magic_comment = None

        match extension.lower():
            case Constants.PY_EXTENSION | Constants.R_EXTENSION:
                header = Constants.PYTHON_HEADER
                comment = Constants.PYTHON_COMMENT
                magic_comment = Constants.PYTHON_MAGIC_COMMENT
                
            case Constants.SQL_EXTENSION:
                header = Constants.SQL_HEADER
                comment = Constants.SQL_COMMENT
                magic_comment = Constants.SQL_MAGIC_COMMENT
                
            case Constants.SCALA_EXTENSION:
                header = Constants.SCALA_HEADER
                comment = Constants.SCALA_COMMENT
                magic_comment = Constants.SCALA_MAGIC_COMMENT

            case _:
                return []
            

        content = content.replace(header, "")
        content = content.replace(magic_comment, "")
        splitted_content = content.split(comment)
        return splitted_content

    def main(self, file_absolute_path: str, file_relative_path: str, output_folder: str) -> None:
        print(f"Info: START Converting the source notebook: '{file_absolute_path}'")
        content = self.__read_notebook(file_absolute_path)
        extension = Path(file_relative_path).suffix.lower()
        cells = self.__get_cells_by_extension(content, extension)
        notebook = create_notebook(cells)
        save_notebook(output_folder, file_relative_path, notebook)
        print(f"Info: FINISH Converting the source notebook: '{file_absolute_path}'")