import logging
import configparser
from pathlib import Path
from snowflake.snowpark import Session
import os
import shortuuid

if not hasattr(Session.SessionBuilder,"___extended"):

    Session.SessionBuilder.___extended = True
    Session.SessionBuilder.__old_create = Session.SessionBuilder.create
    def SessionBuilder_updated_create(self):
        session = self.__old_create()
        if hasattr(self,"__appname__"):
            setattr(session, "__appname__", self.__appname__)
            uuid = shortuuid.uuid()
            session.query_tag = f"APPNAME={session.__appname__};execution_id={uuid}"
        return session
    Session.SessionBuilder.create = SessionBuilder_updated_create
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
                return next(iter(context._active_sessions))
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
        config = configparser.ConfigParser()
        if not os.path.exists(configpath):
            raise Exception(f"No config fount at:{configpath}")
        config.read(configpath)
        section = config["connections" if section is None else f"connections.{section}"]
        connection_parameters = {
            "user":section.get("username"),
            "password":section.get("password"),
            "account":section.get('accountname'),
            "role":section.get('rolename'),
            "warehouse":section.get('warehousename'),
            "database":section.get("dbname"),
            "schema":section.get("schemaname")
        }
        return self.configs(connection_parameters)

    Session.SessionBuilder.from_snowsql = SessionBuilder_snowsql_config

    Session.SessionBuilder.env = SessionBuilder_env