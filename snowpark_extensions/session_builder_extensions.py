import logging
import configparser
from pathlib import Path
from snowflake.snowpark import Session
import os
import shortuuid
import io

if not hasattr(Session.SessionBuilder,"___extended"):         

    _logger = logging.getLogger(__name__)

    def console_handler(stream='stdout'):
        """
        Create a handler for logging to the original console.
        """
        assert stream in {'stdout', 'stderr'}, "stream must be one of 'stdin' or 'stdout'"
        try:
            # Get the file handle of the original std stream.
            fh = getattr(sys, stream)._original_stdstream_copy
            if fh:
                # Create a writable IO stream.
                stream = io.TextIOWrapper(io.FileIO(fh, 'w'))
                # Set up a stream handler.
                return logging.StreamHandler(stream)
        except:
            return None
    _console_handler = console_handler()

    # useful when running from notebooks
    if _console_handler:
        _logger.addHandler(_console_handler)

    Session.SessionBuilder.___extended = True
    Session.SessionBuilder.___create = Session.SessionBuilder.create
    def SessionBuilder_extendedcreate(self):
        session = self.___create()
        if hasattr(self,"__appname__"):
            setattr(session, "__appname__", self.__appname__)
            uuid = shortuuid.uuid()
            session.query_tag = f"APPNAME={session.__appname__};execution_id={uuid}"
        return session
    Session.SessionBuilder.create = SessionBuilder_extendedcreate
    def SessionBuilder_appName(self,name):
        self.__appname__ = name
        return self

    Session.SessionBuilder.appName = SessionBuilder_appName
    def append_tag(self,tag:str):
         session.query_tag = session.query_tag + ";" + tag
    Session.append_tag = append_tag    

    def SessionBuilder_getOrCreate(self):
        from snowflake.snowpark import context
        from snowflake.snowpark.session import _session_management_lock, _active_sessions
        with _session_management_lock:
            if len(_active_sessions) == 1:
                return next(iter(_active_sessions))
        return self.create()

    Session.SessionBuilder.getOrCreate = SessionBuilder_getOrCreate


    def SessionBuilder_env(self):
        self._options["user"]     = os.getenv("SNOW_USER") or os.getenv("SNOWSQL_USER")
        self._options["password"] = os.getenv("SNOW_PASSWORD") or os.getenv("SNOWSQL_PWD")
        self._options["account"]  = os.getenv("SNOW_ACCOUNT") or os.getenv("SNOWSQL_ACCOUNT")
        self._options["role"]     = os.getenv("SNOW_ROLE") or os.getenv("SNOWSQL_ROLE")
        self._options["warehouse"]= os.getenv("SNOW_WAREHOUSE") or os.getenv("SNOWSQL_WAREHOUSE")
        self._options["database"] = os.getenv("SNOW_DATABASE") or os.getenv("SNOWSQL_DATABASE")
        return self

    def SessionBuilder_snowsql_config(self,section=None,configpath=f'{Path.home()}/.snowsql/config'):
        if not os.path.exists(configpath):
            _logger.error(f"No snowsql config found at:{configpath}")
            return self
        config = configparser.ConfigParser()
        config.read(configpath)
        config_section_name = "connections" if section is None else f"connections.{section}"
        if config_section_name in config:
            section = config[config_section_name]
            if section:
                connection_parameters = {
                    "user":section.get("username"),
                    "password":section.get("password"),
                    "account":section.get('accountname'),
                    "role":section.get('rolename'),
                    "warehouse":section.get('warehousename'),
                    "database":section.get("dbname"),
                    "schema":section.get("schemaname")
                }
                self.configs(connection_parameters)
        else:
            _logger.error(f"Config section {config_section_name} not found in snowsql file: {configpath}")
        return self

    Session.SessionBuilder.from_snowsql = SessionBuilder_snowsql_config
    Session.SessionBuilder.from_env = SessionBuilder_env
    Session.SessionBuilder.env = SessionBuilder_env