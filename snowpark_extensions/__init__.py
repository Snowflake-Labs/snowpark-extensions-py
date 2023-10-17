"""Provides Additional Extensions for Snowpark"""


from .dataframe_extensions import *
from .dataframe_reader_extensions import *
from .functions_extensions import *
from .session_extensions import *
from .session_builder_extensions import *
from .types_extensions import *
from .column_extensions import *
from .logging_utils import logged, Tag

def register_sql_magic():
    try:
        from IPython.core.magic import register_cell_magic
        def sql(line, cell):
            import IPython
            user_ns = IPython.get_ipython().user_ns
            if "session" in user_ns:
                session = user_ns['session']
                from jinja2 import Template
                t = Template(cell)
                res = t.render(user_ns)
                name = None
                if line and line.strip():
                    name = line.strip().split(" ")[0]
                df = session.sql(res)
                if name:
                    user_ns[name] = df
                else:
                    user_ns["__df"] = df
                    return df
            else:
                return "No session was found. You can setup one by running: session = Session.builder.from_env().getOrCreate()"
        register_cell_magic(sql)
    except:
        pass

register_sql_magic()