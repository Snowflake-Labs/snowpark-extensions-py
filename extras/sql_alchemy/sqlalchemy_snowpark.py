from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.converter import SnowflakeConverter
from sqlalchemy import create_engine
import sqlalchemy
import logging

# we need to make sure the param style is set to pyformat
snowflake.connector.paramstyle = "pyformat"

from snowflake.snowpark._internal.utils import is_in_stored_procedure

# we mock a few methods
def _process_params_dict(self, params, cursor):
    try:
        res = {k: self._process_single_param(v) for k, v in params.items()}
        return res
    except Exception as e:
        raise Exception("Failed processing pyformat-parameters: {e}")
def _process_params_pyformat(self,params, cursor):
        #todoremove print("----->")
        #todoremove print(params)
        if params is None:
            return {}
        if isinstance(params, dict):
            return self._process_params_dict(params,cursor)
        if not isinstance(params, (tuple, list)):
            params = [params,]
        try:
            res = map(self._process_single_param, params)
            ret = tuple(res)
            return ret
        except Exception as e:
            raise Exception(f"Failed processing pyformat-parameters; {self}{params} {e}")
def _process_single_param(self, param):
        to_snowflake = self.converter.to_snowflake
        escape = self.converter.escape
        _quote = self.converter.quote
        return _quote(escape(to_snowflake(param)))

def create_sqlalchemy_engine(session: Session):
    import snowflake.connector.connection
    from sqlalchemy.engine.url import URL
    from sqlalchemy.engine.base import Connection
    setattr(Connection,"url",URL.create("snowflake"))
    # patch this import
    # patch missing method
    if is_in_stored_procedure():
        # get this from https://github.com/Snowflake-Labs/snowpark-extensions-py/tree/main/extras/wheel_loader
        import wheel_loader         
        wheel_loader.add_wheels() # download wheel from pypi
        snowflake.connector.connection.SnowflakeConnection = snowflake.connector.connection.StoredProcConnection
        setattr(snowflake.connector.connection.StoredProcConnection,"_process_params_pyformat",_process_params_pyformat)
        setattr(snowflake.connector.connection.StoredProcConnection,"_process_params_dict",_process_params_dict)
        setattr(snowflake.connector.connection.StoredProcConnection,"_process_single_param",_process_single_param)
    # Your existing Snowflake connection (replace with your actual connection)
    existing_snowflake_connection = session._conn._conn
    setattr(existing_snowflake_connection,"_interpolate_empty_sequences",False)
    # sql alchemy needs pyformat binding
    existing_snowflake_connection._paramstyle = "pyformat"
    opts = ""
    if session.get_current_warehouse() is not None:
        opts += f"&warehouse={session.get_current_warehouse()}"
    if session.get_current_role() is not None:
        opts += f"&role={session.get_current_role()}"
    conn_url = f"snowflake://{session.get_current_user() or ''}@{session.get_current_account()}/{session.get_current_database() or ''}/{session.get_current_schema() or ''}?{opts}"
    # Create an engine and bind it to the existing Snowflake connection
    engine = create_engine(
        url=conn_url,
        creator=lambda: existing_snowflake_connection
    )
    return engine