from snowflake.snowpark import DataFrame, Row, DataFrameReader
from snowflake.snowpark.types import StructType
from snowflake.snowpark import context
from typing import Any, Union, List, Optional
from snowflake.snowpark.functions import lit
from snowflake.snowpark.dataframe import _generate_prefix

import os
import logging
import pandas as pd
from snowflake.snowpark._internal.utils import TempObjectType, random_name_for_temp_object
import re
from snowflake.snowpark.context import get_active_session

if not hasattr(DataFrameReader,"___extended_csv"):
    import logging    
    DataFrameReader.___extended_csv = True
    def _ingest_csv(self, stage_name: str, warehouse: str = None, min_warehouse_size:str = "XSMALL", max_warehouse_size:str = "XXXLARGE" ) -> str:
        if warehouse is None:
            warehouse = get_active_session().get_current_warehouse()
        pattern = self._cur_options.get("PATTERN","*.csv.*")
        quote   =   self._cur_options.get("FIELD_OPTIONALLY_ENCLOSED_BY",'"')
        sep     =   self._cur_options.get("FIELD_DELIMITER",",")
        # validate stage name
        stage_name = stage_name.strip()
        if not stage_name:
            raise Exception("PLEASE PROVIDE A NON-EMPTY STAGE NAME")
        if stage_name.startswith('@'):
            stage_name = stage_name[1:]
        session = context.get_active_session()
        # Set the warehouse
        session.sql(f"USE WAREHOUSE {warehouse}").collect()
        infer_format = random_name_for_temp_object(TempObjectType.FILE_FORMAT)
        read_format  = random_name_for_temp_object(TempObjectType.FILE_FORMAT)
        table_name   = random_name_for_temp_object(TempObjectType.TABLE)
        adjust_name  = random_name_for_temp_object(TempObjectType.FUNCTION)
        DOLLAR_DOLLAR = "$" + "$"
        # Adjust the schema of the inferred table
        session.sql(f"""
        CREATE OR REPLACE TEMP FUNCTION {adjust_name}(INPUT_ARRAY ARRAY)
        RETURNS ARRAY
        LANGUAGE JAVASCRIPT
        AS
        '
            return INPUT_ARRAY.map(row => {{
                if (row.TYPE.startsWith("NUMBER"))
                {{
                    row.EXPRESSION=`{DOLLAR_DOLLAR}{{row.ORDER_ID + 1}}::DOUBLE`;
                    row.TYPE = `DOUBLE`;
                }}
                return row;
            }});
        ';
            """).collect()

        # file format for inference
        session.sql(f"""
            CREATE OR REPLACE FILE FORMAT {infer_format} 
            TYPE = CSV 
            PARSE_HEADER=TRUE 
            FIELD_DELIMITER = ','
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            COMPRESSION = GZIP
            """).collect()
        # file format for reading
        session.sql(f"""
            CREATE OR REPLACE FILE FORMAT {read_format}
            TYPE = CSV
            SKIP_HEADER=1
            FIELD_DELIMITER = ','
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            COMPRESSION = GZIP;
        """).collect()
        logging.info("File formats created successfully.")
        # List csv files in the stage
        # we will use the first 5 files to infer the schema
        files = [os.path.basename(x[0]) for x in session.sql(f"""list @{stage_name} pattern='.*csv.gz'""").collect()]
        subset = files[:5]
        files_str = ",".join([f"'{x}'" for x in subset ])
        # Create a temporary table to hold the inferred schema
        session.sql(f"""
            CREATE OR REPLACE TEMP TABLE {table_name}
            USING TEMPLATE (
                SELECT {adjust_name}(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION=>'@{stage_name}',
                        FILE_FORMAT=>'{infer_format}',
                        FILES=>({files_str}),
                        max_records_per_file=>1000
                    )
                )
            );
        """).collect()
        logging.info("Temporary table created successfully.")
        # Get the schema of the inferred table
        describe_df = session.sql(f"describe table {table_name}")
        # We will use the schema to create a select statement
        # to read the data from the stage and insert it into the inferred table
        # We perform the following :
        # 1. We try several timestamp formats for the TIMESTAMP/DATE columns
        # 3. We try to convert BOOLEAN columns to BOOLEAN
        # 4. We try to convert NUMBER columns to NUMBER
        # 5. We try to convert DECIMAL columns to DECIMAL
        select_clauses = []
        describe_df = pd.DataFrame([row.as_dict() for row in describe_df.collect()])
        for idx, row in enumerate(describe_df.itertuples(index=False)):
            col_name = row.name
            type_str = row.type.upper()
            col_expr = f"${idx+1}::STRING"

            # Handle TIMESTAMP
            if "TIMESTAMP" in type_str:
                col_expr = (
                    f"COALESCE("
                    f"TRY_TO_TIMESTAMP({col_expr}, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), "
                    f"TRY_TO_TIMESTAMP({col_expr}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')"
                    f")"
                )
            # Handle BOOLEAN
            elif "FLOAT" in type_str:
                col_expr = f"TRY_TO_DOUBLE({col_expr})"
            # Handle BOOLEAN
            elif "BOOLEAN" in type_str:
                col_expr = f"TRY_TO_BOOLEAN({col_expr})"
            # Handle NUMBER and DECIMAL
            elif match := re.match(r"NUMBER\((\d+),(\d+)\)", type_str):
                precision, scale = match.groups()
                col_expr = f"TRY_TO_NUMBER({col_expr}, {precision}, {scale})"
            elif "NUMBER" in type_str:
                col_expr = f"TRY_TO_NUMBER({col_expr})"
            # else: leave as STRING

            select_clauses.append(f"{col_expr} AS \"{col_name}\"")

        # Replace 'INFERRED_TABLE' and 'stage_name' with actual values
        select_statement = f"INSERT INTO {table_name} SELECT\n  " + ",\n  ".join(select_clauses)
        select_statement += f"\nFROM @{stage_name} (FILE_FORMAT=>'{read_format}', PATTERN=>'.*csv.gz');"

        # Now let's scale up the warehouse to handle the data
        session.sql(f"ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = '{max_warehouse_size}'").collect()

        # Execute
        try:
            session.sql(select_statement).collect()
            logging.info("Data ingested successfully.")
            logging.info(f"Files ingested: {len(files)}")
        finally:
            # Scale down the warehouse
            session.sql(f"ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = '{min_warehouse_size}'").collect()
            logging.info("Warehouse scaled down successfully.")
        return session.table(table_name)

    DataFrameReader.ingest_csv = _ingest_csv