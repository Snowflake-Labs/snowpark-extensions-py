import pytest
from snowflake.snowpark import Session, Row, DataFrameReader
from snowflake.snowpark.types import *
from snowflake.snowpark.dataframe import _generate_prefix
import snowpark_extensions

def test_load():    
    session = Session.builder.from_snowsql().getOrCreate()
    cases = session.read.load(["./tests/data/test1_0.csv","./tests/data/test1_1.csv"],
                        schema=get_schema(),
                        format="csv", 
                        sep=",",
                        header="true")
    assert 10 == len(cases.collect())

def test_csv():
    session = Session.builder.from_snowsql().getOrCreate()
    stage = f'{session.get_fully_qualified_current_schema()}.{_generate_prefix("TEST_STAGE")}'
    session.sql(f'CREATE TEMPORARY STAGE IF NOT EXISTS {stage}').show()
    session.file.put(f"file://./tests/data/test1_0.csv", f"@{stage}")
    session.file.put(f"file://./tests/data/test1_1.csv", f"@{stage}")
    dfReader = session.read
    csvInfo = dfReader.csv(f"@{stage}",
                               schema=get_schema(),
                               sep=",",
                               header="true")
    assert 10 == len(csvInfo.collect()) 
    assert dfReader._cur_options["FIELD_DELIMITER"] == ","
    assert dfReader._cur_options["SKIP_HEADER"] == 1

def get_schema():
    schema = StructType([ \
    StructField("case_id",       StringType()), \
    StructField("province",      StringType()), \
    StructField("city",          StringType()), \
    StructField("group",         BooleanType()), \
    StructField("infection_case",StringType()), \
    StructField("confirmed",     IntegerType()), \
    StructField("latitude",      FloatType()), \
    StructField("longitude",     FloatType()) \
    ])
    return schema