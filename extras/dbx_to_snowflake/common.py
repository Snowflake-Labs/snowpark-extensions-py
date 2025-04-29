import nbformat, os
import nbformat
import nbformat.v4 as nb
from nbformat.notebooknode import NotebookNode
from constants import *
from pathlib import Path

# Create a Jupyter notebook        
def create_notebook(cells: list) -> NotebookNode:
    notebook = nb.new_notebook()
    
    for cell in cells:
        stripped_cell = cell.strip()
        magic = None
        language = None
        
        match True:
            case _ if stripped_cell.startswith(Constants.MD_MAGIC):
                magic = Constants.MD_MAGIC
                language = "md"
            case _ if stripped_cell.startswith(Constants.SQL_MAGIC):
                magic = Constants.SQL_MAGIC
                language = "sql"
            case _ if stripped_cell.startswith(Constants.PYTHON_MAGIC):
                magic = Constants.PYTHON_MAGIC
                language = "python"
            case _:
                magic = ""
                language = ""

        stripped_cell = stripped_cell.replace(magic, "")

        if magic == Constants.MD_MAGIC:
            notebook.cells.append(nb.new_markdown_cell(stripped_cell))
            continue

        if magic != "":
            nb_cell = nb.new_code_cell(stripped_cell)
            nb_cell.metadata.update({"language": language})
            notebook.cells.append(nb_cell)
            continue

        notebook.cells.append(nb.new_code_cell(stripped_cell))
    
    return notebook

# Save the Jupyter notebook
def save_notebook(output_folder: str, relative_path: str, notebook: NotebookNode) -> None:
    relative_path = relative_path.replace(Path(relative_path).suffix, Constants.JUPYTER_EXTENSION)
    relative_path = os.path.join(output_folder, relative_path)
    output_file = Path(relative_path)
    output_file.parent.mkdir(exist_ok=True, parents=True)

    with output_file.open("w") as file:
        nbformat.write(notebook, file)