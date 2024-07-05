from snowflake.snowpark import Session, DataFrame
import logging 
from io import StringIO
from snowflake.connector.util_text import split_statements

from snowflake.snowpark._internal.utils import quote_name
from .utils import needs_quoting, RawSqlExpression

class SFContext():
    def __init__(self, session:Session=None):
        self.session = session or Session.builder.getOrCreate()
        self.logger  = logging.getLogger("context")
        self.create_dynamic_frame = SFFrameReader(self)
        self.write_dynamic_frame =  SFFrameWriter(self)

    def create_frame(self,database , table_name ,table_schema="public", transformation_ctx = ""):
        if transformation_ctx:
            self.logger.info(f"CTX:{transformation_ctx}")
            self.session.append_query_tag(transformation_ctx,"|")
        database =  quote_name(database) if needs_quoting(database) else database
        table_name = quote_name(table_name) if needs_quoting(table_name) else table_name
        self.logger.info(f"Reading frame from {database}.{table_schema}.{table_name}")
        return self.session.table([database, table_schema, table_name])

    def run_actions(self, actions_text, kind, fail_on_error=False):
        if actions_text:
            with StringIO(actions_text) as f:
                for statement in split_statements(f, remove_comments=True):
                    try:
                        self.session.sql(statement)
                    except Exception as e:
                        self.logger.error(f"Failed to execute {kind}: {statement}")
                        if fail_on_error:
                            raise e

    def write_frame(self, frame:DataFrame, catalog_connection:str, connection_options:dict, redshift_tmp_dir:str="", transformation_ctx:str = "", write_mode:str="append"):
           if transformation_ctx:
               
               self.session.append_query_tag(transformation_ctx,"|")
           if redshift_tmp_dir:
               self.warning(f"Ignoring argument {redshift_tmp_dir}. Please remove")
           self.logger.info(f"Writing frame to {catalog_connection}")
           preactions = connection_options.get("preactions", "")
           self.run_actions(preactions, "preactions")
           dbtable = connection_options.get("dbtable")
           dbtable =  quote_name(dbtable) if needs_quoting(dbtable) else dbtable
           database = connection_options.get("database")
           database =  quote_name(database) if needs_quoting(database) else database
           frame.write.mode(write_mode).save_as_table([database, dbtable])
           postactions = connection_options.get("postactions", "")
           self.run_actions(postactions, "postactions")

class SFFrameReader(object):
    def __init__(self, context:SFContext):
        self._context = context

    def from_catalog(self, database = None, table_name = None, table_schema="public",redshift_tmp_dir = "", transformation_ctx = "", push_down_predicate = "", additional_options = {}, catalog_id = None, **kwargs):
        """Creates a DynamicFrame with the specified catalog name space and table name.
        """
        if database is None:
            raise Exception("Parameter database is missing.")
        if table_name is None:
            raise Exception("Parameter table_name is missing.")
        db = database
        return self._context.create_frame(database=database,table_name=table_name,table_schema=table_schema,transformation_ctx=transformation_ctx)

class SFFrameWriter(object):
    def __init__(self, context:SFContext):
        self._context = context
    def from_options(self, frame:DataFrame, connection_type, connection_options={},
                       format="parquet", format_options={}, transformation_ctx=""):
        if connection_type == "s3":
            if connection_options.get("storage_integration") is None:
                raise Exception("Parameter storage_integration is missing.")
            storage_integration = connection_options.get("storage_integration")
            frame.write.copy_into_location(connection_options["path"], file_format_type=format, storage_integration=RawSqlExpression(storage_integration), 
            header=True, overwrite=True)
        elif connection_type == "snowflake":
            frame.write.save_as_table(connection_options["path"])
        else:
            raise Exception("Unsupported connection type: %s" % connection_type)
    def from_catalog(self, frame, database = None, table_name = None, table_schema="public", redshift_tmp_dir = "", transformation_ctx = "", additional_options = {}, catalog_id = None, **kwargs):
        if database is None:
            raise Exception("Parameter database is missing.")
        if table_name is None:
            raise Exception("Parameter table_name is missing.")
        db = database
        connection_options = {
            "database": db,
            "dbtable": table_name,
            "schema": table_schema
        }
        return self._context.write_frame(frame,"--", connection_options,transformation_ctx=transformation_ctx)
