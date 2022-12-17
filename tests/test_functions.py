
import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.types import *

def test_create_map():
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
    df = df.withColumn("propertiesMap",create_map(
            lit("salary"),col("salary"),
            lit("location"),col("location")
            )).drop("salary","location")
    df.show()

def test_array_distinct():
    session = Session.builder.appName('test').from_snowsql().getOrCreate()
    df = spark.createDataFrame([([1, 2, 3, 2],), ([4, 5, 5, 4],)], ['data'])
    df.select(array_distinct(df.data)).collect()
