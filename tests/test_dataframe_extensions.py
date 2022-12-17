import pytest
from snowflake.snowpark import Session, Row
import snowpark_extensions
import snowflake.snowpark
from snowflake.snowpark.types import *


def test_applyinpandas():
    session = Session.builder.from_snowsql().getOrCreate()
    import pandas as pd  
    df = session.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        schema=["ID", "V"])
    df1 = df.to_pandas()
    def normalize(pdf):
        V = pdf.V
        return pdf.assign(V=(V - V.mean()) / V.std())
    df2 = normalize(df1)
    # schema can be an string or an StructType
    df.group_by("ID").applyInPandas(
        normalize, schema="id long, v double").show()
    print("done")

def test_explode_with_map():
    from snowflake.snowpark import Session
    import snowpark_extensions
    from snowflake.snowpark.functions import explode
    session = Session.builder.appName('snowpark_extensions_unittest').from_snowsql().getOrCreate()
    schema = StructType([StructField("id", IntegerType()), StructField("an_array", ArrayType()), StructField("a_map", MapType()) ])
    sf_df = session.createDataFrame(
        [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
        schema)
    #  +---+----------+----------+                                                     
    # | id|  an_array|     a_map|
    # +---+----------+----------+
    # |  1|[foo, bar]|{x -> 1.0}|
    # |  2|        []|        {}|
    # |  3|      null|      null|
    # +---+----------+----------+

    results = sf_df.select("id", "an_array", explode("a_map",map=True)).collect()
    # ---------------------------------------
    # |"ID"  |"AN_ARRAY"  |"KEY"  |"VALUE"  |
    # ---------------------------------------
    # |1     |[           |x      |1        |
    # |      |  "foo",    |       |         |
    # |      |  "bar"     |       |         |
    # |      |]           |       |         |
    # ---------------------------------------
    assert len(results) == 1
    assert results[0].ID == 1 and results[0].KEY == 'x' and results[0].VALUE == '1'


def test_explode_outer_with_map():
    from snowflake.snowpark import Session
    import snowpark_extensions
    from snowflake.snowpark.functions import explode_outer
    session = Session.builder.appName('snowpark_extensions_unittest').from_snowsql().getOrCreate()
    schema = StructType([StructField("id", IntegerType()), StructField("an_array", ArrayType()), StructField("a_map", MapType()) ])
    sf_df = session.createDataFrame(
        [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
        schema)
    #  +---+----------+----------+                                                     
    # | id|  an_array|     a_map|
    # +---+----------+----------+
    # |  1|[foo, bar]|{x -> 1.0}|
    # |  2|        []|        {}|
    # |  3|      null|      null|
    # +---+----------+----------+

    results = sf_df.select("id", "an_array", explode_outer("a_map",map=True)).collect()
    # +---+----------+----+-----+
    # | id|  an_array| KEY| VALUE|
    # +---+----------+----+-----+
    # |  1|[foo, bar]|   x|  1 |
    # |  2|        []|null| null|
    # |  3|      null|null| null|
    # +---+----------+----+-----+
    assert len(results) == 3
    assert results[0].ID == 1 and results[0].KEY ==  'x' and results[0].VALUE == '1'
    assert results[1].ID == 2 and results[1].KEY == None and results[1].VALUE == None
    assert results[2].ID == 3 and results[2].KEY == None and results[2].VALUE == None

def test_explode_with_array():
    from snowflake.snowpark import Session
    import snowpark_extensions
    from snowflake.snowpark.functions import explode
    session = Session.builder.appName('snowpark_extensions_unittest').from_snowsql().getOrCreate()
    schema = StructType([StructField("id", IntegerType()), StructField("an_array", ArrayType()), StructField("a_map", MapType()) ])
    sf_df = session.createDataFrame(
        [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
        schema)
    #  +---+----------+----------+                                                     
    # | id|  an_array|     a_map|
    # +---+----------+----------+
    # |  1|[foo, bar]|{x -> 1.0}|
    # |  2|        []|        {}|
    # |  3|      null|      null|
    # +---+----------+----------+

    results = sf_df.select("id", "an_array", explode("an_array")).collect()
    # +---+----------+---+
    # | id|  an_array|col|
    # +---+----------+---+
    # |  1|[foo, bar]|foo|
    # |  1|[foo, bar]|bar|
    # +---+----------+---+
    assert len(results) == 2
    assert results[0].ID == 1 and results[0].COL == '"foo"'
    assert results[1].ID == 1 and results[1].COL == '"bar"'