import pytest
from snowflake.snowpark import Session, Row
import snowpark_extensions
import snowflake.snowpark
from snowflake.snowpark.types import *
from snowflake.snowpark import functions as F

def test_pivot():
    session = Session.builder.from_snowsql().getOrCreate()
    data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
        ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
        ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
        ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]
    df = session.createDataFrame(data, ["Product","Amount","Country"])
    # |"PRODUCT"  |"AMOUNT"  |"COUNTRY"  |
    # ------------------------------------
    # |Banana     |1000      |USA        |
    # |Carrots    |1500      |USA        |
    # |Beans      |1600      |USA        |
    # |Orange     |2000      |USA        |
    # |Orange     |2000      |USA        |
    # |Banana     |400       |China      |
    # |Carrots    |1200      |China      |
    # |Beans      |1500      |China      |
    # |Orange     |4000      |China      |
    # |Banana     |2000      |Canada     |
    res = df.groupBy("Product").pivot("Country").sum("Amount").sort("Product").collect()
    # +-------+------+-----+------+----+                                              
    # |Product|Canada|China|Mexico| USA|
    # +-------+------+-----+------+----+
    # | Banana|  2000|  400|  null|1000|
    # |  Beans|  null| 1500|  2000|1600|
    # |Carrots|  2000| 1200|  null|1500|
    # | Orange|  null| 4000|  null|4000|
    # +-------+------+-----+------+----+
    assert len(res)==4
    assert res[0][0]=='Banana'  and res[0][1]==2000 and res[0][2]==400  and res[0][3]==None and res[0][4]==1000
    assert res[1][0]=='Beans'   and res[1][1]==None and res[1][2]==1500 and res[1][3]==2000 and res[1][4]==1600
    assert res[2][0]=='Carrots' and res[2][1]==2000 and res[2][2]==1200 and res[2][3]==None and res[2][4]==1500
    assert res[3][0]=='Orange'  and res[3][1]==None and res[3][2]==4000 and res[3][3]==None and res[3][4]==4000

def test_pivot_with_numbers_as_columns():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([
    (20741434 ,53  ,'  ', '2021-07-21 07:21:35.172000'),  
    (20741454 ,10  ,'MM', '2021-07-21 07:21:35.173000'),  
    (20741467 ,6   ,'MC', '2021-07-21 07:21:35.173000'),  
    (20741474 ,11  ,'32', '2021-07-21 07:21:35.174000'),  
    (20741496 ,28  ,'EA', '2021-07-21 07:21:35.174000'),  
    (20741511 ,464 ,'  ', '2021-07-21 07:21:35.175000'),  
    (20741544 ,1   ,'GG', '2021-07-21 07:21:35.175000'),  
    (20741560 ,46  ,'NN', '2021-07-21 07:21:35.176000'),  
    (20741583 ,464 ,'  ', '2021-07-21 07:21:35.177000'),  
    (20741598 ,618 ,'3P', '2021-07-21 07:21:35.177000')    
    ], ['A','B','C','D'])
    df2 = (
        df.groupBy("A").pivot("B").agg(F.min("C"))
    )
    # -----------------------------------------------------------------------------
    # |"A"       |"1"   |"6"   |"10"  |"11"  |"28"  |"46"  |"53"  |"464"  |"618"  |
    # -----------------------------------------------------------------------------
    # |20741583  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |       |NULL   |
    # |20741496  |NULL  |NULL  |NULL  |NULL  |EA    |NULL  |NULL  |NULL   |NULL   |
    # |20741544  |GG    |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL   |NULL   |
    # |20741434  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |      |NULL   |NULL   |
    # |20741598  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL   |3P     |
    # |20741511  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |NULL  |       |NULL   |
    # |20741454  |NULL  |NULL  |MM    |NULL  |NULL  |NULL  |NULL  |NULL   |NULL   |
    # |20741474  |NULL  |NULL  |NULL  |32    |NULL  |NULL  |NULL  |NULL   |NULL   |
    # |20741467  |NULL  |MC    |NULL  |NULL  |NULL  |NULL  |NULL  |NULL   |NULL   |
    # |20741560  |NULL  |NULL  |NULL  |NULL  |NULL  |NN    |NULL  |NULL   |NULL   |
    # -----------------------------------------------------------------------------
    res = df2.collect()
    assert len(res)==10
    assert df2.columns == ['A','"1"','"6"','"10"','"11"','"28"','"46"','"53"','"464"','"618"']


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
    res = df.group_by("ID").applyInPandas(
        normalize, schema="ID long, V double",batch_size=2).orderBy("V").collect()
    assert len(res)==5
    assert str(res[0].V) == '-0.8320502943378437'
    assert str(res[1].V) == '-0.7071067811865475'
    assert str(res[2].V) == '-0.2773500981126146'
    assert str(res[3].V) == '0.7071067811865475'
    assert str(res[4].V) == '1.1094003924504583'


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
    assert results[0].ID == 1 and results[0][2] == 'x' and results[0][3] == '1'


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
    assert results[0].ID == 1 and results[0][2] ==  'x' and results[0][3] == '1'
    assert results[1].ID == 2 and results[1][2] == None and results[1][3] == None
    assert results[2].ID == 3 and results[2][2] == None and results[2][3] == None

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

    results = sf_df.select("id", "an_array", explode("an_array").alias("COL")).collect()
    # +---+----------+---+
    # | id|  an_array|col|
    # +---+----------+---+
    # |  1|[foo, bar]|foo|
    # |  1|[foo, bar]|bar|
    # +---+----------+---+
    assert len(results) == 2
    assert results[0].ID == 1 and results[0].COL == '"foo"'
    assert results[1].ID == 1 and results[1].COL == '"bar"'


def test_explode_outer_with_array():

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

    results = sf_df.select("id", "an_array", explode_outer("an_array").alias("col")).collect()
    # +---+----------+----+
    # | id|  an_array| col|
    # +---+----------+----+
    # |  1|[foo, bar]| foo|
    # |  1|[foo, bar]| bar|
    # |  2|        []|null|
    # |  3|      null|null|
    # +---+----------+----+
    assert len(results) == 4
    assert results[0].ID == 1 and results[0].COL == '"foo"'
    assert results[1].ID == 1 and results[1].COL == '"bar"'
    assert results[2].ID == 2 and results[2].COL == None
    assert results[3].ID == 3 and results[3].COL == None

def test_array_zip():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, None, 3],),([1],),([],)], ['data'])
    # +---------------+
    # |           data|
    # +---------------+
    # |[2, null, 3]   |
    # |            [1]|
    # |             []|
    # +---------------+
    df = df.withColumn("FIELDS", F.arrays_zip("data","data"))
    # +------------+------------------------------+
    # |data        |FIELDS                        |
    # +------------+------------------------------+
    # |[2, null, 3]|[{2, 2}, {null, null}, {3, 3}]|
    # |[1]         |[{1, 1}]                      |
    # |[]          |[]                            |
    # +------------+------------------------------+
    res = df.collect()
    assert len(res)==3
    res1 = eval(res[0][1].replace("null","None"))
    res2 = eval(res[1][1].replace("null","None"))
    res3 = eval(res[2][1])
    assert res1==[[2,2],[None,None],[3,3]]
    assert res2==[[1,1]]
    assert res3==[]
    df = df.withColumn("FIELDS", F.arrays_zip("data","data","data")).orderBy("data")
    res = df.collect()
    res1 = eval(res[0][1].replace("null","None"))
    res2 = eval(res[1][1].replace("null","None"))
    res3 = eval(res[2][1].replace("null","None"))
    assert len(res)==3
    assert res1==[]
    assert res2==[[1,1,1]]
    assert res3==[[2,2,2],[None,None,None],[3,3,3]]
    


def test_nested_specials():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, None, 3],),([1],),([],)], ['data'])
    #df2 = df.withColumn("FIELDS", F.arrays_zip("data","data"))
    df = df.withColumn("FIELDS", F.explode_outer(F.arrays_zip("data","data")))
    res = df.collect()
    # +------------+------------+
    # |        data|      FIELDS|
    # +------------+------------+
    # |[2, null, 3]|      {2, 2}|
    # |[2, null, 3]|{null, null}|
    # |[2, null, 3]|      {3, 3}|
    # |         [1]|      {1, 1}|
    # |          []|        null|
    # +------------+------------+    
    assert len(res)==5
    array1=eval(res[0]['FIELDS'])
    array2=eval(res[1]['FIELDS'].replace("null","None"))
    array3=eval(res[2]['FIELDS'])
    array4=eval(res[3]['FIELDS'])
    array5=res[4]['FIELDS']
    assert array1 == [2,2]
    assert array2 == [None,None]
    assert array3 == [3,3]
    assert array4 == [1,1]
    assert array5 == None


def test_stack():
#  +-------+---------+-----+---------+----+
#  |   Name|Analytics|   BI|Ingestion|  ML|
#  +-------+---------+-----+---------+----+
#  | Mickey|     null|12000|     null|8000|
#  | Martin|     null| 5000|     null|null|
#  |  Jerry|     null| null|     1000|null|
#  |  Riley|     null| null|     null|9000|
#  | Donald|     1000| null|     null|null|
#  |   John|     null| null|     1000|null|
#  |Patrick|     null| null|     null|1000|
#  |  Emily|     8000| null|     3000|null|
#  |   Arya|    10000| null|     2000|null|
#  +-------+---------+-----+---------+----+     
    session = Session.builder.from_snowsql().getOrCreate()
    data0 = [
    ('Mickey' , None,12000,None,8000),
    ('Martin' , None, 5000,None,None),
    ('Jerry'  , None, None,1000,None),
    ('Riley'  , None, None,None,9000),
    ('Donald' , 1000, None,None,None),
    ('John'   , None, None,1000,None),
    ('Patrick', None, None,None,1000),
    ('Emily'  , 8000, None,3000,None),
    ('Arya'   ,10000, None,2000,None)]

    schema_df = StructType([
    StructField('Name'      ,  StringType(), True),
    StructField('Analytics' , IntegerType(), True),
    StructField('BI'        , IntegerType(), True),
    StructField('Ingestion' , IntegerType(), True),
    StructField('ML'        , IntegerType(), True)
    ])

    df = session.createDataFrame(data0,schema_df)
    df.show()
    unstacked = df.select("NAME",df.stack(4,F.lit('Analytics'), "ANALYTICS", F.lit('BI'), "BI", F.lit('Ingestion'), "INGESTION", F.lit('ML'), "ML").alias("Project", "Cost_To_Project"))
    res = unstacked.collect()
    assert len(res) == 36
    res = unstacked.filter(F.col("Cost_To_Project").is_not_null()).orderBy("NAME","Project").collect()
    assert len(res) == 12
    assert list(res[ 0]) == ['Arya', 'Analytics', 10000]
    assert list(res[ 1]) == ['Arya', 'Ingestion', 2000]
    assert list(res[ 2]) == ['Donald', 'Analytics', 1000]
    assert list(res[ 3]) == ['Emily', 'Analytics', 8000]
    assert list(res[ 4]) == ['Emily', 'Ingestion', 3000]
    assert list(res[ 5]) == ['Jerry', 'Ingestion', 1000]
    assert list(res[ 6]) == ['John', 'Ingestion', 1000]
    assert list(res[ 7]) == ['Martin', 'BI', 5000]
    assert list(res[ 8]) == ['Mickey', 'BI', 12000]
    assert list(res[ 9]) == ['Mickey', 'ML', 8000]
    assert list(res[10]) == ['Patrick', 'ML', 1000]
    assert list(res[11]) == ['Riley', 'ML', 9000]
        
