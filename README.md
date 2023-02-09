# snowpark-extensions-py

Snowpark by itself is a powerful library, but still some utility functions can always help.

# Installation

We recommended installing using [PYPI](https://pypi.org/)

```bash

    $ pip install snowconvert-deploy-tool --upgrade
```    
> note:: If you run this command on MacOS change `pip` by `pip3`


# Usage


just import it at the top of your file and it will automatically extend your snowpark package.
For example:
``` python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.from_snowsql().appName("app1").getOrCreate()
```
```

## Currently provided extensions:

## SessionBuilder extensions

| Name                          | Description |
| ----------------------------- | ----------- |
| SessionBuilder.from_snowsql   | can read the information from the snowsql config file by default at ~/snowsql/config or at a given location |
| SessionBuilder.env            | reads settings from SNOW_xxx or SNOWSQL_xxx variables |
| SessionBuilder.appName        | Sets a query tag with the given appName               |
| SessionBuilder.append_tag     | Appends a new tag to the existing query tag           | 


You can the create your session like:

``` python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.from_snowsql().appName("app1").create()
```

``` python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.env().appName("app1").create()
```

The `appName` can use to setup a query_tag like `APPNAME=tag;execution_id=guid` which can then be used to track job actions with a query like

You can then use a query like:
To see all executions from an app or 
```sql
select *
from table(information_schema.query_history())
whery query_tag like '%APPNAME=tag%'
order by start_time desc;
```

To see the executions for a particular execution:

```sql
select *
from table(information_schema.query_history())
whery query_tag like '%APPNAME=tag;execution_id=guid%'
order by start_time desc;
```

## Column Extensions
| Name                         | Description                                                                         |
|------------------------------|-------------------------------------------------------------------------------------|
| Column.getItem               | An expression that gets an item at position ordinal out of a list, or gets an item by key out of a dict. |


## DataFrame Extensions

| Name                           | Description                                                                         |
|--------------------------------|-------------------------------------------------------------------------------------|
| DataFrame.dtypes               | returns the list of datatypes in the DataFrame
| DataFrame.map                  | provides an equivalent for the map function for example `df.map(func,input_types=[StringType(),StringType()],output_types=[StringType(),IntegerType()],to_row=True)` 
| DataFrame.simple_map           | if a simple lambda like `lambda x: x.col1 + x.col2` is used this functions can be used like `df.simple_map(lambda x: x.col1 + x.col2)`
| DataFrame.groupby.applyInPandas| Maps each group of the current DataFrame using a pandas udf and returns the result as a DataFrame. |
| DataFrame.replace        | extends replace to allow using a regex
| DataFrame.groupBy.pivot        | extends the snowpark groupby to add a pivot operator
| DataFrame.stack  | This is an operator similar to the unpivot operator

### Examples

#### map and simple_map

```python
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
import snowpark_extensions

session = Session.builder.from_snowsql().appName("app1").getOrCreate()
  
data = [('James','Smith','M',30),('Anna','Rose','F',41),('Robert','Williams','M',62)]
columns = ["firstname","lastname","gender","salary"]
df = session.createDataFrame(data=data, schema = columns)
df.show()


#
#--------------------------------------------------
#|"FIRSTNAME"  |"LASTNAME"  |"GENDER"  |"SALARY"  |
#--------------------------------------------------
#|James        |Smith       |M         |30        |
#|Anna         |Rose        |F         |41        |
#|Robert       |Williams    |M         |62        |
#--------------------------------------------------




# using map with a lamda, the to_row indicates that the code will pass a row as x to the lambda
# if you have a lambda like lambda x,y,z you can use to_row=False
df2=df.map(lambda x: 
        (x[0]+","+x[1],x[2],x[3]*2),
        output_types=[StringType(),StringType(),IntegerType()],to_row=True)
df2.show()

#
#-----------------------------------
#|"C_1"            |"C_2"  |"C_3"  |
#-----------------------------------
#|James,Smith      |M      |60     |
#|Anna,Rose        |F      |82     |
#|Robert,Williams  |M      |124    |
#-----------------------------------
#

# for simple lambda
# simple map will just pass the same dataframe to the function
# this approach is faster
df2 = df.simple_map(lambda x: (x[0]+","+x[1],x[2],x[3]*2))
df2.toDF(["name","gender","new_salary"]).show()

#---------------------------------------------
#|"NAME"           |"GENDER"  |"NEW_SALARY"  |
#---------------------------------------------
#|James,Smith      |M         |60            |
#|Anna,Rose        |F         |82            |
#|Robert,Williams  |M         |124           |
#---------------------------------------------
#

```

### replace with support for regex

```python
df = session.createDataFrame([('bat',1,'abc'),('foo',2,'bar'),('bait',3,'xyz')],['A','C','B'])
# already supported replace
df.replace(to_replace=1, value=100).show()
# replace with regex
df.replace(to_replace=r'^ba.$', value='new',regex=True).show()
```

### applyInPandas
```python
from snowflake.snowpark import Session
import snowpark_extensions
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
```

```
------------------------------
|"ID"  |"V"                  |
------------------------------
|2     |-0.8320502943378437  |
|2     |-0.2773500981126146  |
|2     |1.1094003924504583   |
|1     |-0.7071067811865475  |
|1     |0.7071067811865475   |
------------------------------
```

### stack

Assuming you have a DataTable like:

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

```python
df.select("NAME",df.stack(4,lit('Analytics'), "ANALYTICS", lit('BI'), "BI", lit('Ingestion'), "INGESTION", lit('ML'), "ML").alias("Project", "Cost_To_Project")).filter(col("Cost_To_Project").is_not_null()).orderBy("NAME","Project")
```

That will return:
```
'-------------------------------------------
|"NAME"   |"PROJECT"  |"COST_TO_PROJECT"  |
-------------------------------------------
|Arya     |Analytics  |10000              |
|Arya     |Ingestion  |2000               |
|Donald   |Analytics  |1000               |
|Emily    |Analytics  |8000               |
|Emily    |Ingestion  |3000               |
|Jerry    |Ingestion  |1000               |
|John     |Ingestion  |1000               |
|Martin   |BI         |5000               |
|Mickey   |BI         |12000              |
|Mickey   |ML         |8000               |
|Patrick  |ML         |1000               |
|Riley    |ML         |9000               |
-------------------------------------------
```

## DataFrameReader Extensions

| Name                           | Description                                                                         |
|--------------------------------|-------------------------------------------------------------------------------------|
| DataFrameReader.format         | Specified the format of the file to load
| DataFrameReader.load           | Loads a dataframe from a file. It will upload the files to an stage if needed 

### Example


## Functions Extensions

| Name                         | Description                                                                         |
|------------------------------|-------------------------------------------------------------------------------------|
| functions.array_sort         | sorts the input array in ascending order or descending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array. |
| ~~functions.unix_timestamp~~     | ~~returns the UNIX timestamp of current time.~~ **Available in snowpark-python >= 1.1.0**                                         |
| ~~functions.from_unixtimestamp~~ | ~~can be used to convert UNIX time to Snowflake timestamp~~ **Available in snowpark-python >= 1.1.0**                             |
| functions.format_number      | formats numbers using the specified number of decimal places                        |
| functions.reverse            | returns a reversed string                                                           |
| functions.explode            | returns a new row for each element in the given array
| functions.explode_outer      | returns a new row for each element in the given array or map. Unlike explode, if the array/map is null or empty then null is produced    |
| functions.arrays_zip         | returns a merged array of arrays |
| functions.array_sort         | sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.
| functions.array_max          | returns the maximon value of the array.
| functions.array_min          | returns the minimum value of the array.
| functions.array_distinct     | removes duplicate values from the array.
| function.daydiff             | this function returns the difference in days between two dates. This function will be direct equivalent of the spark datediff. You can simple replace spark datediff by daydiff 
| functions.date_add           | returns the date that is n days days after                                          |
| functions.date_sub           | returns the date that is n days before                                              |
| functions.regexp_extract     | extract a specific group matched by a regex, from the specified string column.      |
| functions.regexp_split     | splits a specific group matched by a regex, it is an extension of split wich supports a limit parameter.       |
| ~~functions.asc~~                | ~~returns a sort expression based on the ascending order of the given column name.~~ **Available in snowpark-python >=1.1.0**    |
| ~~functions.desc~~               | ~~returns a sort expression based on the descending order of the given column name.~~ **Available in snowpark-python >=1.1.0**    |
| functions.flatten            | creates a single array from an array of arrays
| functions.sort_array         | sorts the input array in ascending or descending order according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order
| functions.map_values         | Returns an unordered array containing the values of the map. |
| functions.struct             | Returns an object built with the given columns |
| functions.bround             | This function receives a column with a number and rounds it to scale decimal places with HALF_EVEN round mode, often called as "Banker's rounding" . This means that if the number is at the same distance from an even or odd number, it will round to the even number. |


### Examples:

#### array_sort

```python
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark import functions as F
import snowpark_extensions

session = Session.builder.from_snowsql().getOrCreate()
df = session.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
df.select(F.array_sort(df.data)).show()
```

```
------------
|"SORTED"  |
------------
|[         |
|  1,      |
|  2,      |
|  3,      |
|  null    |
|]         |
|[         |
|  1       |
|]         |
|[]        |
------------
```

### explode and explode_outer

Snowflake builtin [FLATTEN](https://docs.snowflake.com/en/sql-reference/functions/flatten.html) provide the same functionality, but the explode syntax can be somethings easier. This helper provide the same syntax. 
> NOTE: explode can be used with arrays and maps/structs. In this helper at least for now you need to specify if you want to process this as array or map. We provide explode and explode outer our you can just use explode with the outer=True flag.

```python
from snowflake.snowpark import Session
import snowpark_extensions
from snowflake.snowpark.functions import explode
session = Session.builder.appName('snowpark_extensions_unittest').from_snowsql().getOrCreate()
schema = StructType([StructField("id", IntegerType()), StructField("an_array", ArrayType()), StructField("a_map", MapType()) ])
sf_df = session.createDataFrame([(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],schema)
```
```
#  +---+----------+----------+                                                     
# | id|  an_array|     a_map|
# +---+----------+----------+
# |  1|[foo, bar]|{x -> 1.0}|
# |  2|        []|        {}|
# |  3|      null|      null|
# +---+----------+----------+
```
```python
sf_df.select("id", "an_array", explode("an_array")).show()
```
```
# +---+----------+---+
# | id|  an_array|col|
# +---+----------+---+
# |  1|[foo, bar]|foo|
# |  1|[foo, bar]|bar|
# +---+----------+---+
```
```python
sf_df.select("id", "an_array", explode_outer("an_array")).show()
```

```
# +---+----------+----+
# | id|  an_array| COL|
# +---+----------+----+
# |  1|[foo, bar]| foo|
# |  1|[foo, bar]| bar|
# |  2|        []|    |
# |  3|          |    |
# +---+----------+----+
```

For a map use
```python
results = sf_df.select("id", "an_array", explode_outer("an_array",map=True))
```

```
# +---+----------+----+-----+
# | id|  an_array| KEY| VALUE|
# +---+----------+----+-----+
# |  1|[foo, bar]|   x|   1 |
# |  2|        []|    |     |
# |  3|          |    |     |
# +---+----------+----+-----+
```


### regexp_extract

```python
session = Session.builder.from_snowsql().create()


df = session.createDataFrame([('100-200',)], ['str'])
res = df.select(F.regexp_extract('str',r'(\d+)-(\d+)',1).alias('d')).collect()
print(str(res))
# [Row(D='1')]

df = session.createDataFrame([['id_20_30', 10], ['id_40_50', 30]], ['id', 'age'])
df.show()
# --------------------
# |"ID"      |"AGE"  |
# --------------------
# |id_20_30  |10     |
# |id_40_50  |30     |
# --------------------


df.select(F.regexp_extract('id', r'(\d+)', 1)).show()
# ------------------------------------------------------
# |"COALESCE(REGEXP_SUBSTR(""ID"", '(\\D+)', 1, 1,...  |
# ------------------------------------------------------
# |20                                                  |
# |40                                                  |
# ------------------------------------------------------


df.select(F.regexp_extract('id', r'(\d+)_(\d+)', 2)).show()
# ------------------------------------------------------
# |"COALESCE(REGEXP_SUBSTR(""ID"", '(\\D+)_(\\D+)'...  |
# ------------------------------------------------------
# |30                                                  |
# |50                                                  |
# ------------------------------------------------------
```

### regexp_split

```python
session = Session.builder.from_snowsql().create()

df = session.createDataFrame([('oneAtwoBthreeC',)], ['s',])
res = df.select(regexp_split(df.s, '[ABC]', 2).alias('s')).collect()
print(str(res))
# [\n  "one",\n  "twoBthreeC"\n]
```

# utilities

| Name | Description         |
|------|---------------------|
| utils.map_to_python_type | maps from DataType to python type |
| utils.map_string_type_to_datatype | maps a type by name to a snowpark `DataType` |
| utils.schema_str_to_schema | maps an schema specified as an string to a `StructType()`


# Experimental

## Jupyter Notebook support

```
%load_ext snowpark_extensions
```

This extension provides simple integration with Jupyter notebooks
It will preload the snowpark libraries and adds a simple magic.

A `%%sql` magic can be used to run queries. 
Queries can use `Jinja2` syntax. For example:

If a previous cell you had something like:
```python
COL1=1
```
Then on following cells you can do:

```sql
%%sql
select * from tables where col={{COL1}}
```

You can use give a name that you can use later for example:

```sql
%%sql tables
select * from information_schema.tables limit 5
```

and then use that as a normal dataframe:
```python
if tables.count() > 5:
    print("There are more that 5 tables")
```
If you dont specify a name you can still access the last result using `__df`.

> NOTE: By default only 50 rows are displays. You can customize this limit for example to 100 rows with:
```
import snowpark_extensions
snowpark_extensions.rows_limit = 100
```

You can configure Jupyter to run some imports and initialization code at the start of a notebook by creating a file called `startup.ipy` in the `~/.ipython/profile_default/startup` directory. 

Any code written in this file will be executed when you start a new Jupyter notebook.

An [example startup.ipy](https://github.com/MobilizeNet/snowpark-extensions-py/blob/main/startup.ipy) is provided


## Running notebooks in Snowpark

There is an small script that uploads your notebook to snowflake into an stage and executes it. It will record an html with the results of the notebook execution.

To use it:

- install snowpark_extensions
- runner --notebook notebook1.ipynb --stage my_stage --package ""