
import pytest
import snowpark_extensions
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col,lit, array_sort,sort_array, array_max, array_min, map_values, struct,object_construct, array_agg, bround
from snowflake.snowpark import functions as F
import re

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
    array2 = eval(res[1]['FLATTEN'].replace("null",'None'))
    assert array1[0]==1 and array1[1]==2 and array1[2]==3 and array1[3]==4 and array1[4]==5 and array1[5]==6
    assert array2 == [1, None, 4, 5] 

def test_create_map():
    def do_assert(res):
        assert len(res) == 5
        assert res[0].ID == '34534' and res[0].DEPT == "Sales"     and eval(res[0].PROPERTIESMAP)['location'] == 'USA' and eval(res[0].PROPERTIESMAP)['salary'] == 6500
        assert res[1].ID == '36636' and res[1].DEPT == "Finance"   and eval(res[1].PROPERTIESMAP)['location'] == 'USA' and eval(res[1].PROPERTIESMAP)['salary'] == 3000
        assert res[2].ID == '39192' and res[2].DEPT == "Marketing" and eval(res[2].PROPERTIESMAP)['location'] == 'CAN' and eval(res[2].PROPERTIESMAP)['salary'] == 2500
        assert res[3].ID == '40288' and res[3].DEPT == "Finance"   and eval(res[3].PROPERTIESMAP)['location'] == 'IND' and eval(res[3].PROPERTIESMAP)['salary'] == 5000
        assert res[4].ID == '42114' and res[4].DEPT == "Sales"     and eval(res[4].PROPERTIESMAP)['location'] == 'USA' and eval(res[4].PROPERTIESMAP)['salary'] == 3900
    session = Session.builder.app_name('test').from_snowsql().getOrCreate()
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


def test_map_values():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.sql("SELECT object_construct('1', 'a', '2', 'b') as data")
    res = df.select(map_values("data").alias("values")).collect()
    # +------+
    # |values|
    # +------+
    # |[a, b]|
    # +------+
    assert len(res)==1
    array=re.sub(r"\s","",res[0].VALUES)
    assert array == '["a","b"]'
    df = session.sql("SELECT object_construct('1', 'value1', '2', parse_json('null')) as data")
    res = df.select(map_values("data").alias("values")).collect()
    assert len(res)==1
    array=re.sub(r"\s","",res[0].VALUES)
    assert array == '["value1",null]'

def test_struct():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([('Bob', 80), ('Alice', None)], ["name", "age"])
    res=df.select(struct('age', 'name').alias("struct")).collect()    
#     [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    assert len(res)==2
    assert re.sub(r"\s","",res[0].STRUCT) == '{"age":80,"name":"Bob"}'
    assert re.sub(r"\s","",res[1].STRUCT) == '{"age":null,"name":"Alice"}'
    res = df.select(struct([df.age, df.name]).alias("struct")).collect()
    assert len(res)==2
    assert re.sub(r"\s","",res[0].STRUCT) == '{"AGE":80,"NAME":"Bob"}'
    assert re.sub(r"\s","",res[1].STRUCT) == '{"AGE":null,"NAME":"Alice"}'    
#     [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    res = df.select(struct(df.age.alias("A"), df.name.alias("B")).alias("struct")).collect()
    assert len(res)==2
    assert re.sub(r"\s","",res[0].STRUCT) == '{"A":80,"B":"Bob"}'
    assert re.sub(r"\s","",res[1].STRUCT) == '{"A":null,"B":"Alice"}'

def test_daydiff():
    session = Session.builder.from_snowsql().getOrCreate()
    df = session.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])
    res = df.select(F.daydiff(F.to_date(df.d2), F.to_date(df.d1)).alias('diff')).collect()
    assert res[0].DIFF == 32  


def test_bround():
    session = Session.builder.from_snowsql().getOrCreate()
    data0 = [(1.5,0),
    (2.5,0),
    (0.00,0),
    (0.5,0),
    (-1.5,0),
    (-2.5,0)]

    data1 = [
    (2.25,1),
    (2.65,1),
    (0.00,1),
    (1.05,1),
    (1.15,1),
    (-2.25,1),
    (-2.35,1),
    (None,1),
    (1.5,1),
    (1.5,-1) ]

    data_null = [
    (0.5,None),
    (1.5,None),
    (2.5,None),
    (-1.5,None),
    (-2.5,None),
    (None,None)]
    schema_df = StructType([
    StructField('value', FloatType(), True),
    StructField('scale', IntegerType(), True)
    ])
    
    df_0 = session.createDataFrame(data0, schema_df)
    df_1 = session.createDataFrame(data1, schema_df)
    df_null = session.createDataFrame(data_null, schema_df)

    res0 = df_0.withColumn("rounding",F.bround(F.col('value').cast(DecimalType(20,4)),0)).collect()
    assert len(res0) == 6
    assert res0[0].ROUNDING == 2.0
    assert res0[1].ROUNDING == 2.0
    assert res0[2].ROUNDING == 0.0
    assert res0[3].ROUNDING == 0.0
    assert res0[4].ROUNDING == -2.0
    assert res0[5].ROUNDING == -2.0

    res1 = df_1.withColumn("rounding",F.bround(F.col('value').cast(DecimalType(20,4)),1)).collect()
    assert len(res1) == 10
    assert str(res1[0].ROUNDING) == "2.2"
    assert str(res1[1].ROUNDING) == "2.6"
    assert str(res1[2].ROUNDING) == "0.0"
    assert str(res1[3].ROUNDING) == "1.0"
    assert str(res1[4].ROUNDING) == "1.2"
    assert str(res1[5].ROUNDING) == "-2.2"
    assert str(res1[6].ROUNDING) == "-2.4"
    assert str(res1[7].ROUNDING) == "None"
    assert str(res1[8].ROUNDING) == "1.5"
    assert str(res1[9].ROUNDING) == "1.5"

    resNull = df_null.withColumn("rounding",F.bround(F.col('value').cast(DecimalType(20,4)),None) ).collect()
    assert len(resNull) == 6
    assert str(resNull[0].ROUNDING) == "None"
    assert str(resNull[1].ROUNDING) == "None"
    assert str(resNull[2].ROUNDING) == "None"
    assert str(resNull[3].ROUNDING) == "None"
    assert str(resNull[4].ROUNDING) == "None"
    assert str(resNull[5].ROUNDING) == "None"

def test_regexp_split():
    session = Session.builder.from_snowsql().config("schema","PUBLIC").getOrCreate()
    from snowflake.snowpark.functions import regexp_split
    df = session.createDataFrame([('testAandtestBareTwoBBtests',)], ['s',])
    res = df.select(regexp_split(df.s, "test(A|BB)" , 3).alias('s')).collect()
    assert res[0].S == '[\n  "",\n  "andtestBareTwoBBtests"\n]'
    res = df.select(regexp_split(df.s, "test(A|BB)", 1).alias('s')).collect()
    assert res[0].S == '[\n  "testAandtestBareTwoBBtests"\n]'

    df = session.createDataFrame([('From: mauricio@mobilize.net',)], ['s',])

    res = df.select(regexp_split(df.s, "((From|To)|Subject): (\w+@\w+\.[a-z]+)").alias('s')).collect()
    assert res[0].S == '[\n  "",\n  ""\n]'

    df = session.createDataFrame([('oneAtwoBthreeC',)], ['s',])
    
    res = df.select(regexp_split(df.s, 'Z').alias('s')).collect()
    assert res[0].S == '[\n  "oneAtwoBthreeC"\n]'
    res = df.select(regexp_split(df.s, 't').alias('s')).collect()
    assert res[0].S == '[\n  "oneA",\n  "woB",\n  "hreeC"\n]'
    res = df.select(regexp_split(df.s, 't', 1).alias('s')).collect()
    assert res[0].S == '[\n  "oneAtwoBthreeC"\n]'
    res = df.select(regexp_split(df.s, 't', 2).alias('s')).collect()
    assert res[0].S == '[\n  "oneA",\n  "woBthreeC"\n]'    
    res = df.select(regexp_split(df.s, '[ABC]').alias('s')).collect()
    assert res[0].S == '[\n  "one",\n  "two",\n  "three",\n  ""\n]'    
    res = df.select(regexp_split(df.s, '[ABC]', 1).alias('s')).collect()
    assert res[0].S == '[\n  "oneAtwoBthreeC"\n]'  
    res = df.select(regexp_split(df.s, '[ABC]', 2).alias('s')).collect()
    assert res[0].S == '[\n  "one",\n  "twoBthreeC"\n]'   
    
    df = session.createDataFrame([('HelloabNewacWorld',)], ['s',])

    res = df.select(regexp_split(df.s, 'a([b, c]).*?').alias('s')).collect()
    assert res[0].S == '[\n  "Hello",\n  "New",\n  "World"\n]'

    df = session.createDataFrame([(r'aa\nbb\nccc\b',)], ['s',])
    
    res = df.select(regexp_split(df.s, r'\w+.').alias('s')).collect()
    assert res[0].S == '[\n  "",\n  "",\n  "",\n  "b"\n]'

    df = session.createDataFrame([(r'\n\n\n',)], ['s',])   

    res = df.select(regexp_split(df.s, '.*', 3).alias('s')).collect()
    assert res[0].S == '[\n  "",\n  "",\n  ""\n]'

    df = session.createDataFrame([("""line 1
line 2
line 3""",)], ['s',])    

    res = df.select(regexp_split(df.s, r'\n', 3).alias('s')).collect()
    assert res[0].S == '[\n  "line 1",\n  "line 2",\n  "line 3"\n]'
    res = df.select(regexp_split(df.s, r'line 1(\n)', 3).alias('s')).collect()
    assert res[0].S == '[\n  "",\n  "line 2\\nline 3"\n]'

    df = session.createDataFrame([('The price of PINEAPPLE ice cream is 20',)], ['s',])
    res = df.select(regexp_split(df.s, r"(\b[A-Z]+\b).+(\b\d+)", 4).alias('s')).collect()
    assert res[0].S == '[\n  "The price of ",\n  ""\n]'

    df = session.createDataFrame([('<button type="submit" class="btn">Send</button>',)], ['s',])
    res = df.select(regexp_split(df.s, '".+?"', 4).alias('s')).collect()
    assert res[0].S == '[\n  "<button type=",\n  " class=",\n  ">Send</button>"\n]'

def test_to_utc_timestamp_ext():
    session = Session.builder.from_snowsql().config("schema","PUBLIC").getOrCreate()
    from snowflake.snowpark.functions import to_utc_timestamp_ext
    df = session.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
    res = df.select(F.to_utc_timestamp_ext(df.ts, "PST").alias('utc_time')).collect()
    assert res[0][0] == datetime.datetime(1997, 2, 28, 18, 30)
    res = df.select(F.to_utc_timestamp_ext(df.ts, df.tz).alias('utc_time')).collect()
    assert res[0][0] == datetime.datetime(1997, 2, 28, 1, 30)