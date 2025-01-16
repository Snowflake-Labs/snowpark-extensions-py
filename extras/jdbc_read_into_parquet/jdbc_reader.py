from snowflake.snowpark import Session, DataFrameReader
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, object_construct
import json

if not hasattr(DataFrameReader, "__jdbc_reader__"):
    setattr(DataFrameReader, "__jdbc_reader__", True)
    class JdbcDataFrameReader:
        def __init__(self):
            self.options = {}
        def option(self,key:str,value:str):
            if key == "dbtable":
                return self.query("select * from " + str(value))
            self.options[key] = value
            return self
        def options(self,new_opts:dict):
            self.options.update(new_opts)
            return self
        def query(self,sql:str):
            self.query_stmt = sql
            return self
        def load(self):
            session = get_active_session()    
            #result_table = session.sql(f"""CALL READ_JDBC2(PARSE_JSON($${json.dumps(self.options)}$$)::OBJECT,$${self.query_stmt}$$) """).first()[0]
            result_table = "LOADED_TABLE"
            session.sql(f"""CALL LOAD_TO_TABLE(PARSE_JSON($${json.dumps(self.options)}$$)::OBJECT,$${self.query_stmt}$$,'LOADED_TABLE')""").first()[0]
            return session.table(result_table)
    def format(self,format_name):
            return JdbcDataFrameReader() if format_name == "jdbc" else Exception("not supported")
    def jdbc(self, table=None,properties:dict={}):
        reader = format("jdbc")
        if table:
            reader = reader.option("dbtable",table)
        return reader.options(properties)
DataFrameReader.format = format
DataFrameReader.jdbc   = jdbc

