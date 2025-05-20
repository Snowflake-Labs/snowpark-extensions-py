
import pytest
import snowpark_extensions
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col,lit, array_sort,sort_array, array_max, array_min, map_values, struct,object_construct, array_agg, bround
from snowflake.snowpark import functions as F
import re

#def test_ingest_csv():
session = Session.builder.from_snowsql().getOrCreate()
df = session.read.option("delimiter",",").ingest_csv("SNOWPARK_TESTDB.PUBLIC.BUG1","XSMALL_WH")
df.show()


