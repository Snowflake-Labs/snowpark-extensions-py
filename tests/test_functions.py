
import pytest
import snowpark_extensions
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col,lit, sort_array, array_max, array_min
from snowflake.snowpark import functions as F


def test_asc():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    res = df.select(df.name).orderBy(F.asc(df.name)).collect()
    assert res[0].NAME == 'Alice'

def test_desc():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    res = df.select(df.name).orderBy(F.desc(df.name)).collect()
    assert res[0].NAME == 'Tom'

def test_array():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.sql("select 1 A")
    res=df.withColumn("array",F.array(lit(1),lit(2),lit(3))).collect()
    assert len(res)==1
    array = eval(res[0][1])
    assert array[0]==1 and array[1]==2 and array[2]==3

def test_array_distinct():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.sql("select 1 A")
    df=df.withColumn("array",F.array(lit(1),lit(1),lit(1),lit(2),lit(3),lit(2),lit(2)))
    res=df.withColumn("array_d",F.array_distinct("ARRAY")).collect()
    assert len(res)==1
    array = eval(res[0][2])
    assert len(array)==3
    assert array[0]==1 and array[1]==2 and array[2]==3

def test_array_flatten():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([[1, 2, 3], [4, 5], [6]],1), ([[1],None, [4, 5]],2)], ['data','pos'])
    res=df.select(F.flatten(df.data).alias("FLATTEN"))
    res=res.collect()
    assert len(res)==2
    array1 = eval(res[0]['FLATTEN'])
    array2 = eval(res[1]['FLATTEN'] or 'None')
    assert array1[0]==1 and array1[1]==2 and array1[2]==3 and array1[3]==4 and array1[4]==5 and array1[5]==6
    assert array2 is None 

def test_create_map():
    def do_assert(res):
        assert len(res) == 5
        assert res[0].ID == '34534' and res[0].DEPT == "Sales"     and eval(res[0].PROPERTIESMAP)['location'] == 'USA' and eval(res[0].PROPERTIESMAP)['salary'] == 6500
        assert res[1].ID == '36636' and res[1].DEPT == "Finance"   and eval(res[1].PROPERTIESMAP)['location'] == 'USA' and eval(res[1].PROPERTIESMAP)['salary'] == 3000
        assert res[2].ID == '39192' and res[2].DEPT == "Marketing" and eval(res[2].PROPERTIESMAP)['location'] == 'CAN' and eval(res[2].PROPERTIESMAP)['salary'] == 2500
        assert res[3].ID == '40288' and res[3].DEPT == "Finance"   and eval(res[3].PROPERTIESMAP)['location'] == 'IND' and eval(res[3].PROPERTIESMAP)['salary'] == 5000
        assert res[4].ID == '42114' and res[4].DEPT == "Sales"     and eval(res[4].PROPERTIESMAP)['location'] == 'USA' and eval(res[4].PROPERTIESMAP)['salary'] == 3900
    session = Session.builder.appName('test').from_snowsql().getOrCreate()
    data = [ ("36636","Finance",3000,"USA"), 
        ("40288","Finance",5000,"IND"), 
        ("42114","Sales",3900,"USA"), 
        ("39192","Marketing",2500,"CAN"), 
        ("34534","Sales",6500,"USA") ]
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('dept', StringType(), True),
        StructField('salary', IntegerType(), True),
        StructField('location', StringType(), True)
        ])
    df = session.createDataFrame(data=data,schema=schema)
    df.show()
    #Convert columns to Map
    from snowflake.snowpark.functions import col,lit,create_map
    res = df.withColumn("propertiesMap",create_map(
            lit("salary"),col("salary"),
            lit("location"),col("location")
            )).drop("salary","location").sort("ID").collect()
    do_assert(res)
    res=df.withColumn("propertiesMap",create_map([
            lit("salary"),col("salary"),
            lit("location"),col("location")]
            )).drop("salary","location").sort("ID").collect()
    do_assert(res)
    res=df.withColumn("propertiesMap",create_map((
            lit("salary"),col("salary"),
            lit("location"),col("location"))
            )).drop("salary","location").sort("ID").collect()
    do_assert(res)
    res=df.withColumn("propertiesMap",create_map((
            (lit("salary"),col("salary")),
            (lit("location"),col("location"))
            ))).drop("salary","location").sort("ID").collect()
    do_assert(res)

def test_array_sort():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, 1, None, 3],1),([1],2),([],3)], ['data','pos'])
    res = df.select(df.pos,F.array_sort(df.data)).orderBy(col('pos')).collect()
    assert len(res) == 3
    array1 = eval(res[0][1].replace("null","None"))
    array2 = eval(res[1][1].replace("null","None"))
    array3 = eval(res[2][1].replace("null","None"))
    assert array1 == [1,2,3,None]
    assert array2 == [1]
    assert array3 == []

def test_sort_array():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
    res  = df.select(sort_array(df.data).alias('r')).collect()
    # [Row(r=[None, 1, 2, 3]), Row(r=[1]), Row(r=[])]
    assert res[0].R == ',1,2,3' and res[1].R == '1' and res[2].R==''
    res = df.select(sort_array(df.data, asc=False).alias('r')).collect()
    #[Row(r=[3, 2, 1, None]), Row(r=[1]), Row(r=[])]
    assert res[0].R=='3,2,1,' and res[1].R == '1' and res[2].R==''

def test_array_max():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
    res = df.select(array_max(df.data).alias('max')).collect()
    #[Row(max=3), Row(max=10)]
    assert len(res)==2
    assert res[0].MAX == '3'
    assert res[1].MAX == '10'

def test_array_min():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
    res=df.select(array_min(df.data).alias('min')).collect()
    assert res[0].MIN == '1' and res[1].MIN == '-1'
    #[Row(min=1), Row(min=-1)]
