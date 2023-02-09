import pytest
from snowflake.snowpark import Session, Row
from snowflake.snowpark.types import *
import snowpark_extensions

def test_load():    
    session = Session.builder.from_snowsql().getOrCreate()
    schema = StructType([ \
    StructField("case_id",       StringType()), \
    StructField("province",      StringType()), \
    StructField("city",          StringType()), \
    StructField("group",         BooleanType()), \
    StructField("infection_case",StringType()), \
    StructField("confirmed",     IntegerType()), \
    StructField("latitude",      FloatType()), \
    StructField("cilongitudety", FloatType()) \
    ])
    cases = session.read.load(["./tests/data/test1_0.csv","./tests/data/test1_1.csv"],
                        schema=schema,
                        format="csv", 
                        sep=",",
                        header="true")
    assert 10 == len(cases.collect())