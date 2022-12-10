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

## Functions Extensions

| Name                         | Description                                                                         |
|------------------------------|-------------------------------------------------------------------------------------|
| functions.array_sort         | sorts the input array in ascending order or descending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array. |
| functions.unix_timestamp     | returns the UNIX timestamp of current time.                                         |
| functions.from_unixtimestamp | can be used to convert UNIX time to Snowflake timestamp                             |
| functions.format_number      | formats numbers using the specified number of decimal places                        |
| functions.reverse            | returns a reversed string                                                           |
| functions.explode            | returns a new row for each element in the given array                               |
| functions.date_add           | returns the date that is n days days after                                          |
| functions.date_sub           | returns the date that is n days before                                              |
| functions.regexp_extract     | Extract a specific group matched by a regex, from the specified string column.      |


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
-------------------------------------------
|"ARRAY_SORT(""DATA"", TRUE :: BOOLEAN)"  |
-------------------------------------------
|[                                        |
|  2,                                     |
|  1,                                     |
|  3,                                     |
|  null                                   |
|]                                        |
|[]                                       |
|[                                        |
|  1                                      |
|]                                        |
-------------------------------------------
```
df.select(F.array_sort(df.data, asc=False)).show()

```
--------------------------------------------
|"ARRAY_SORT(""DATA"", FALSE :: BOOLEAN)"  |
--------------------------------------------
|[                                         |
|  1                                       |
|]                                         |
|[]                                        |
|[                                         |
|  null,                                   |
|  2,                                      |
|  1,                                      |
|  3                                       |
|]                                         |
--------------------------------------------
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

# utilities

| Name | Description         |
|------|---------------------|
| utils.map_to_python_type | maps from DataType to python type |
| utils.map_string_type_to_datatype | maps a type by name to a snowpark `DataType` |
| utils.schema_str_to_schema | maps an schema specified as an string to a `StructType()`




