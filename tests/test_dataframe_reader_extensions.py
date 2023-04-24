import pytest
from snowflake.snowpark import Session, Row, DataFrameReader
from snowflake.snowpark.types import *
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
    csvInfo = session.read.csv(["./tests/data/test1_0.csv","./tests/data/test1_1.csv"],
                               schema=get_schema(),
                               sep=",",
                               header="true")
    assert 10 == len(csvInfo.collect())

def get_schema():
    schema = StructType([ \
    StructField("case_id",       StringType()), \
    StructField("province",      StringType()), \
    StructField("city",          StringType()), \
    StructField("group",         BooleanType()), \
    StructField("infection_case",StringType()), \
    StructField("confirmed",     IntegerType()), \
    StructField("latitude",      FloatType()), \
    StructField("longitude", FloatType()) \
    ])
    return schema