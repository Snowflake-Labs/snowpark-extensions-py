"""Provides Additional Extensions for Snowpark"""


from .dataframe_extensions import *
from .dataframe_reader_extensions import *
from .functions_extensions import *
from .session_builder_extensions import *
from .types_extensions import *
from .column_extensions import *


rows_limit = 50

def get_display_html() -> None:
    import inspect
    for frame in inspect.getouterframes(inspect.currentframe()):
        global_names = set(frame.frame.f_globals)
        # Use multiple functions to reduce risk of mismatch
        if all(v in global_names for v in ["displayHTML", "display", "spark"]):
            return frame.frame.f_globals["displayHTML"]
    raise Exception("Unable to detect displayHTML function")

def load_ipython_extension(ipython):
    def instructions():
        inst = """
import pandas as pd
from snowflake.snowpark import Session
import snowpark_extensions
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark import functions as F
        """
        return inst 
    error_message_template="""<div style="background-color: #f44336; color: white; padding: 16px;"><strong>Error:</strong> <span id="error-message">@error</span></div>"""
    output_cell_output = None
    try:
        output_cell_output = get_display_html()
        print("displayHTML detected")
    except:
        from IPython.display import display, HTML
        output_cell_output = lambda x: display(HTML(x))

    def snowflake_dataframe_formatter(df):
        # Format the dataframe as a table using pandas
        from snowflake.snowpark.exceptions import SnowparkSQLException
        try:
            count = df.count()
            if count == 0:
                print(f"0 rows were returned.")
            elif count == 1:
                r=df.collect()
                import pandas as pd
                pandas_df = pd.DataFrame.from_records([x.as_dict() for x in r])
                return pandas_df.to_html()
            elif count > rows_limit:
                print(f"There are {count} rows. Showing only {rows_limit} ")
                return df.limit(rows_limit).to_pandas().to_html()
            else:
                return df.to_pandas().to_html()
        except SnowparkSQLException as sce:
            error_msg = sce.message
            formatted = error_message_template.replace("@error", error_msg)
            return formatted
        except Exception as ex:
            error_message = str(ex)
            return f"<pre>{error_message}</pre>"
   


    # Register the display hook
    from snowflake.snowpark import DataFrame
    get_ipython().display_formatter.formatters['text/html'].for_type(DataFrame, snowflake_dataframe_formatter)

    from IPython.core.magic import (Magics, magics_class, cell_magic)
    @magics_class
    class SnowparkMagics(Magics):
        def __init__(self, shell):
            super(SnowparkMagics, self).__init__(shell)
        @cell_magic
        def sql(self, line, cell):
            if "session" in self.shell.user_ns:
                session = self.shell.user_ns['session']
                from jinja2 import Template
                t = Template(cell)
                res = t.render(self.shell.user_ns)
                name = None
                if line and line.strip():
                    name = line.strip().split(" ")[0]
                df = session.sql(res)
                if name:
                    self.shell.user_ns[name] = df
                else:
                    self.shell.user_ns["__df"] = df
                    return df
            else:
                return "No session was found. You can setup one by running: session = Session.builder.from_env().getOrCreate()"
    magics = SnowparkMagics(ipython)
    ipython.register_magics(magics)
    compiled_file = "prep_log.py"
    code = compile(instructions(), compiled_file, 'exec')
    async def main():
        await ipython.run_code(code,{})
    import nest_asyncio
    nest_asyncio.apply()
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())

