# Databricks Notebook Converter Tool

This tool is designed to convert Databricks notebooks into Jupyter notebooks (`.ipynb`) or other source formats such as Python, SQL, Scala, and Markdown. It supports both `.dbc` archive files and individual source files, making it easier to work with Databricks notebooks outside of the Databricks environment.

## Features

- **Convert `.dbc` files**: Extract and convert Databricks `.dbc` archive files into Jupyter notebooks.
- **Convert source files**: Convert individual Python, SQL, Scala, or R source files into Jupyter notebooks.
- **Preserve notebook structure**: Retains the structure and content of the original Databricks notebook, including code cells and markdown cells.

## Requirements

- Python 3.10 or higher
- Required Python libraries:
  - `nbformat`

Install the required Python libraries:
`pip install -r /path/to/requirements.txt`

## Usage
Command-Line Interface
Run the tool using the main.py script. The script accepts an input folder containing .dbc or source files and an output folder where the converted notebooks will be saved.

`python main.py -i <input-folder> -o <output-folder>`

### Arguments:
- `-i`, `--input`: Path to the folder containing .dbc or source files.
- `-o`, `--output`: Path to the folder where the converted notebooks will be saved.

## Supported File Types
- `.dbc`: Databricks archive files
- `.py`: Python source files
- `.sql`: SQL source files
- `.scala`: Scala source files
- `.r`: R source files

## File Structure
- `main.py`: Entry point for the tool. Handles argument parsing and orchestrates the conversion process.
- `dbc_to_jupyter.py`: Handles the conversion of .dbc files to Jupyter notebooks.
- `source_to_jupyter.py`: Handles the conversion of individual source files to Jupyter notebooks.
- `common.py`: Contains shared utility functions for creating and saving Jupyter notebooks.
- `constants.py`: Defines constants used throughout the tool.

## Output
The converted notebooks are saved in the specified output folder using the Jupyter Notebook syntax (`.ipynb`). Each notebook retains the structure and content of the original Databricks notebook.