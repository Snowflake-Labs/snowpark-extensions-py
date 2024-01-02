create or replace procedure run_snowsql_script(file_location STRING, root_location STRING DEFAULT NULL, VARS VARIANT DEFAULT {})
    returns String
    language python
    runtime_version = 3.8
    packages =(
        'rich==13.3.5',
        'snowflake-snowpark-python==*',
        'snowflake-telemetry-python==0.2.0'
    )
    handler = 'run_snowsql_script'
    as '
# This script implements a helper that can be use with snowpark to run scripts using the SnowSQL syntax
# Runs a script using the SnowSQL syntax


from snowflake.snowpark import Session
from snowflake.snowpark.types import StructField, StructType, StringType
from snowflake.snowpark.functions import col
from snowflake.connector.util_text import split_statements
from io import StringIO
from snowflake.snowpark._internal.utils import is_in_stored_procedure
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark.files import SnowflakeFile
import re
import logging
from rich import print as pprint
from rich.console import Console
from rich.syntax import Syntax
console = Console()

import os
supported_commands = ["!abort", "!define", "!exit", "!pause", "!print", "!quit", "!result", "!set", "!source", "!spool", "!system", "!variables"]

class ExitScripts(Exception):
    pass

current_variables = {}
current_options = {}
variable_substitution=False
root_location = ""
# Configure logging to write to a file
# logging.basicConfig(filename=''example.log'', level=logging.INFO)

class Interpolator:
    _simple_re = re.compile(r''(?<!\\\\)\\&([A-Za-z0-9_]+)'')
    _extended_re = re.compile(
        r''(?<!\\\\)\\&\\{([A-Za-z0-9_]+)((:?-)([^}]+))?\\}'')
    def __init__(self, dict_values,fail_on_unresolved: bool=False) -> None:
        self.fail_on_unresolved = fail_on_unresolved
        self.dict_values = dict_values

    def _repl_simple_env_var(self) -> str:
        def substitute(m):
            var_name = m.group(1)
            result = self.dict_values.get(var_name)
            if result is None and self.fail_on_unresolved:
                raise RuntimeError(f"Var not found {var_name}")
            return result or ''''

        return substitute

    def _repl_extended_env_var(self) -> str:
        def substitute(m):
            var_name = m.group(1)
            result = self.dict_values.get(var_name)
            default_spec = m.group(2)
            if default_spec:
                default_raw = m.group(4)
                if m.group(3) == '':-'':
                    # use default if var is unset or empty
                    if not result:
                        result = Interpolator._simple_re.sub(
                            self._repl_simple_env_var(),
                            default_raw)
                elif m.group(3) == ''-'':
                    # use default if var is unset
                    if result is None:
                        result = Interpolator._simple_re.sub(
                            self._repl_simple_env_var(),
                            default_raw)
                else:
                    raise RuntimeError(''Unexpected string matched regex'')
            else:
                if result is None and self.fail_on_unresolved:
                    raise RuntimeError(f"Unresolved variable {var_name}")
            return result or ''''

        return substitute

    def render(self, template: str) -> str:
        # handle simple un-bracketed env vars like $FOO
        first_pass = Interpolator._simple_re.sub(
            self._repl_simple_env_var(),
            template)

        # handle bracketed env vars with optional default specification
        return Interpolator._extended_re.sub(
            self._repl_extended_env_var(),
            first_pass)

interpolator = Interpolator(current_variables)

# Redirect stdout to the logger
class LoggerWriter:
    def write(self, message):
        if message != ''\\n'':
            logging.info(message.rstrip())
    def flush(*args):
        pass

# Replace sys.stdout with LoggerWriter
import sys
sys.stdout = LoggerWriter()

def create_table(data_dict):
    max_key_length = max(map(len, data_dict.keys()))
    max_value_length = max(map(lambda x: len(str(x)), data_dict.values()))

    table_str = "+{}+{}+\\n".format(''-'' * (max_key_length + 2), ''-'' * (max_value_length + 2))
    table_str += "|{:<{}}|{:<{}}|\\n".format("Key", max_key_length, "Value", max_value_length)
    table_str += "+{}+{}+\\n".format(''-'' * (max_key_length + 2), ''-'' * (max_value_length + 2))

    for key, value in data_dict.items():
        table_str += "|{:<{}}|{:<{}}|\\n".format(key, max_key_length, str(value), max_value_length)

    table_str += "+{}+{}+".format(''-'' * (max_key_length + 2), ''-'' * (max_value_length + 2))

    return table_str

def get_file_contents(session, file_path) -> str:
   return "\\n".join([(x[0] or "") for x in session.read.schema(StructType([StructField("LINE",StringType())])).csv(file_path).collect()])

def run_command(session,command, statement_txt):
    global variable_substitution
    global root_location
    statement_rest = statement_txt[len(command):].strip()
    if command == "!abort":
        session.connection.execute_string(f"SELECT SYSTEM$CANCEL_QUERY(''{statement_rest}''")
    elif command == "!define" or command == "!set":
        var_name  = statement_rest[:statement_rest.index("=")]
        var_value = statement_rest[statement_rest.index("=")+1:].strip()
        if var_name.lower() == ''variable_substitution'':
            variable_substitution = var_value.lower() == ''true''
        else:
            current_variables[var_name] = var_value
    elif command == "!exit" or command == "!quit":
        raise ExitScripts()
    elif command == "!pause":
        print("[yellow] Pause ignored")
        logging.info("Sorry pause is not supported ignoring")
    elif command == "!print":
        console.print(f"[blue]{statement_rest}[/blue]")
        logging.info(statement_rest)
    elif command == "!result":
        session.sql(result_scan_statement(statement_rest))
    elif command == "!source" or command == "!load":
        run_snowsql_script_base(session, os.path.join(root_location,statement_rest))
    elif command == "!spool":
        logging.info("in progress")
    elif command == "!system":
        logging.error(f"Not supported command system: {statement_rest}")
    elif command == "!variables":
        logging.info(create_table(current_variables))

def process_statements(session,statements:list):
    current_line = 1
    for statement_txt, type in statements:
        if variable_substitution:
            statement_txt = interpolator.render(statement_txt)
        try:
            line_count = len(statement_txt.splitlines())
            if type is None:
                # None is set if the statement is empty or comment only.
                pass
            elif type == False:
                # False means valid statement.
                statement_txt = statement_txt.strip()
                if statement_txt.startswith("!"):
                    for command in supported_commands:
                        if statement_txt.lower().startswith(command):
                            run_command(session,command,statement_txt)
                else:
                    syntax = Syntax(statement_txt, "sql",line_numbers=True, start_line=current_line)
                    console.print(syntax)
                    session.connection.execute_string(statement_txt)
            elif type == True:
                # The is_put_or_get is set to True if the statement is PUT or GET
                logging.error("PUT or GET support not implemented yet")
            current_line += line_count
        except ExitScripts as exitscripts:
            raise exitscripts
        except Exception as e:
            pprint(f"[red]{e}[/red]")
            logging.error(f"Error processing statement: {statement_txt}. Error: {e}")

def setup_telemetry(file_location):
    try:
        from snowflake import telemetry
        telemetry.set_span_attribute(file_location, "begin")
    except:
        logging.error("Telemetry not installed")

def add_telemetry_info(info_dict):
    try:
        from snowflake import telemetry
        telemetry.add_event("script info", info_dict)
    except:
        logging.error("Telemetry not installed")

def run_snowsql_script_base(session: Session, file_location:str, new_root_location=None,vars:dict={})->str:
    """
    Runs a script using the SnowSQL syntax
    :param session: Snowpark session
    :param file_location: location of the script
    :return:
    """
    global root_location
    if new_root_location is not None:
        root_location=new_root_location
    pprint(f"START EXECUTION of: [cyan]{file_location}[/cyan]")
    current_variables.update(vars)
    setup_telemetry(file_location)
    contents = get_file_contents(session, file_location)
    statements = list(split_statements(StringIO(contents)))
    add_telemetry_info({"filename": file_location, "LOC": len(contents.splitlines()), "statements": len(statements), "variables":current_variables, "options": current_options})
    process_statements(session, statements)
    final_message=f"Done executing script {file_location}"
    logging.info(final_message)
    return final_message


def run_snowsql_script(session: Session, file_location:str, new_root_location:str, vars: dict)->str:
    from datetime import datetime
    current_datetime = datetime.now()
    try:
        run_snowsql_script_base(session, file_location, new_root_location, vars)
    except ExitScripts:
        print("Exiting")
    current_session_id = session.sql("select CURRENT_SESSION()").collect()[0][0]
    return f"Execution snowsql script {file_location} in session {current_session_id} started at: {current_datetime} finished"';