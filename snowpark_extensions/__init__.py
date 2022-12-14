"""Provides Additional Extensions for Snowpark"""


from .dataframe_extensions import *
from .functions_extensions import *
from .session_builder_extensions import *
from .types_extensions import *
from .column_extensions import *

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
                    self.shell.user_ns["$df"] = df
                from IPython.core.display import display, HTML
                display(HTML(df.to_pandas().to_html()))
            else:
                return "No session was found"
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