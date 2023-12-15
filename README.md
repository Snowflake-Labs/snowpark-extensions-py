# snowpark-extensions-py

Snowpark by itself is a powerful library, but still some utility functions can always help.


BTW what about `Java`/ `Scala`/ `SQL` ? There is an [additional repo](https://github.com/Snowflake-Labs/snowpark-extensions "Snowpark Extensions for Java, Scala and SQL") where you will have also utility functions and extensions for those technologies.

**NOTE: we have been working to integrate some of the snowpark extensions directly into the snowpark-python library.
In most cases the APIs will be exactly the same, so there should no changes needed in your code.
However there might be breaking changes, so consider that before updating.
If any of these breaking changes are affecting you, please enter an issue so we can address it.**

# Installation

We recommended installing using [PYPI](https://pypi.org/)

```bash

    $ pip install snowpark-extensions
```

> note:: If you run this command on MacOS change `pip` by `pip3`

# Usage

just import it at the top of your file and it will automatically extend your snowpark package.
For example:

```python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.from_snowsql().appName("app1").getOrCreate()
```

## Currently provided extensions:

## Session Extensions

Session was extened to support IPython display. Using session as a value in a cell will display the session info

## SessionBuilder extensions

| Name                            | Description                                                                                                 |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| SessionBuilder.from_snowsql     | can read the information from the snowsql config file by default at ~/snowsql/config or at a given location |
| SessionBuilder.env              | reads settings from SNOW_xxx or SNOWSQL_xxx variables                                                       |
| SessionBuilder.appName          | Sets a query tag with the given appName                                                                     |
| SessionBuilder.append_tag       | Appends a new tag to the existing query tag                                                                 |
| ~~SessionBuilder.getOrCreate~~ | **Available in snowpark-python >= 1.3.0**                                                            |

You can the create your session like:

```python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.from_snowsql().appName("app1").create()
```

```python
from snowflake.snowpark import Session
import snowpark_extensions
new_session = Session.builder.env().appName("app1").create()
```

> NOTE: since 1.8.0 the [python connector was updated](https://docs.snowflake.com/en/release-notes/clients-drivers/python-connector-2023#version-3-1-0-july-31-2023) and we provide support for an unified configuration storage for `snowflake-python-connector` and `snowflake-snowpark-python` with this approach.
>
> You can use this connections leveraging `Session.builder.getOrCreate()` or `Session.builder.create()`
>
> By default, we look for the `connections.toml` file in the location specified in the `SNOWFLAKE_HOME` environment variable (default: `~/.snowflake`). If this folder does not exist, the Python connector looks for the file in the `platformdirs` location, as follows:
>
> * On Linux: `~/.config/snowflake/`, but follows XDG settings
> * On Mac: `~/Library/Application Support/snowflake/`
> * On Windows: `%USERPROFILE%\AppData\Local\snowflake\`
>
> The default connection by default is 'default' but it can be controlled with the environment variable: `SNOWFLAKE_DEFAULT_CONNECTION_NAME`.
>
> If you dont want to use a file you can set the file contents thru the `SNOWFLAKE_CONNECTIONS` environment variable.
>
> Connection file looks like:
>
> ```
> [default]
> account = "myaccount"
> user = "user1"
> password = 'xxxxx'
> role = "user_role"
> database = "demodb"
> schema = "public"
> warehouse = "load_wh"
>
>
> [snowpark]
> account = "myaccount"
> user = "user2"
> password = 'yyyyy'
> role = "user_role"
> database = "demodb"
> schema = "public"
> warehouse = "load_wh"
>
> ```

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

| Name                | Description                                                                                                                                                         |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ~~Column.getItem~~ | ~~An expression that gets an item at position ordinal out of a list, or gets an item by key out of a dict.~~  **Available in snowpark-python >= 1.3.0**  |

## DataFrame Extensions

| Name                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ~~DataFrame.dtypes~~                | ~~returns the list of datatypes in the DataFrame ~~**Available in snowpark python >= 1.1.0**                                                                                                                                                                                                                                                                                                                             |
| DataFrame.map                        | provides an equivalent for the map function for example `df.map(func,input_types=[StringType(),StringType()],output_types=[StringType(),IntegerType()],to_row=True)`                                                                                                                                                                                                                                                          |
| DataFrame.simple_map                 | if a simple lambda like `lambda x: x.col1 + x.col2` is used this functions can be used like `df.simple_map(lambda x: x.col1 + x.col2)`                                                                                                                                                                                                                                                                                      |
| ~~DataFrame.groupby.applyInPandas~~ | Maps each group of the current DataFrame using a pandas udf and returns the result as a DataFrame.<br />`applyInPandas` overload is kept to avoid breaking changes. But we recommend using the [native](https://docs.snowflake.com/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.RelationalGroupedDataFrame.apply_in_pandas) `apply_in_pandas`<br />**Available in snowpark-python >= 1.8.0** |
| DataFrame.replace                    | extends replace to allow using a regex                                                                                                                                                                                                                                                                                                                                                                                          |
| DataFrame.groupBy.pivot              | extends the snowpark groupby to add a pivot operator                                                                                                                                                                                                                                                                                                                                                                            |
| DataFrame.stack                      | This is an operator similar to the unpivot operator                                                                                                                                                                                                                                                                                                                                                                             |

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
```

```
--------------------------------------------------
|"FIRSTNAME"  |"LASTNAME"  |"GENDER"  |"SALARY"  |
--------------------------------------------------
|James        |Smith       |M         |30        |
|Anna         |Rose        |F         |41        |
|Robert       |Williams    |M         |62        |
--------------------------------------------------
```

```python
# using map with a lambda, the to_row indicates that the code will pass a row as x to the lambda
# if you have a lambda like lambda x,y,z you can use to_row=False
df2=df.map(lambda x: 
        (x[0]+","+x[1],x[2],x[3]*2),
        output_types=[StringType(),StringType(),IntegerType()],to_row=True)
df2.show()
```

```
-----------------------------------
|"C_1"            |"C_2"  |"C_3"  |
-----------------------------------
|James,Smith      |M      |60     |
|Anna,Rose        |F      |82     |
|Robert,Williams  |M      |124    |
-----------------------------------
```

```python
# for simple lambda
# simple map will just pass the same dataframe to the function
# this approach is faster
df2 = df.simple_map(lambda x: (x[0]+","+x[1],x[2],x[3]*2))
df2.toDF(["name","gender","new_salary"]).show()
```

```
---------------------------------------------
|"NAME"           |"GENDER"  |"NEW_SALARY"  |
---------------------------------------------
|James,Smith      |M         |60            |
|Anna,Rose        |F         |82            |
|Robert,Williams  |M         |124           |
---------------------------------------------
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

> NOTE: since snowflake-snowpark-python==1.8.0 applyInPandas is available. This version is kept because:
>
> 1. It supports string schemas
> 2. It automatically wraps the column names. In snowpark applyInPandas you need to do:
>
>    ```
>    def func(pdf):
>        pdf.columns = ['columnname1','columnname2']
>        # rest of the code
>    ```
>
>    Before using your function, to guarantee the proper names are used. This implementation will just use the DF column names. Take in consideration that this still might imply changes as metadata in SF is upper case and lowercase references like df['v'] might fail.
>
> In general it is recommended you use the the snowpark built-in. The extensions only overwrite `applyInPandas`, the `apply_in_pandas` refers to the official snowpark implementation

### stack

Assuming you have a DataTable like:

```
  +-------+---------+-----+---------+----+
  |   Name|Analytics|   BI|Ingestion|  ML|
  +-------+---------+-----+---------+----+
  | Mickey|     null|12000|     null|8000|
  | Martin|     null| 5000|     null|null|
  |  Jerry|     null| null|     1000|null|
  |  Riley|     null| null|     null|9000|
  | Donald|     1000| null|     null|null|
  |   John|     null| null|     1000|null|
  |Patrick|     null| null|     null|1000|
  |  Emily|     8000| null|     3000|null|
  |   Arya|    10000| null|     2000|null|
  +-------+---------+-----+---------+----+  
```

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

| Name                   | Description                                                                   |
| ---------------------- | ----------------------------------------------------------------------------- |
| DataFrameReader.format | Specified the format of the file to load                                      |
| DataFrameReader.load   | Loads a dataframe from a file. It will upload the files to an stage if needed |

### Example

## Functions Extensions

| Name                              | Description                                                                                                                                                                                                                                                                                                              |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| functions.array_sort              | sorts the input array in ascending order or descending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.                                                                                                                                          |
| ~~functions.unix_timestamp~~     | ~~returns the UNIX timestamp of current time.~~ **Available in snowpark-python >= 1.1.0**                                                                                                                                                                                                                         |
| ~~functions.from_unixtimestamp~~ | ~~can be used to convert UNIX time to Snowflake timestamp~~ **Available in snowpark-python >= 1.1.0**                                                                                                                                                                                                             |
| functions.to_utc_timestamp        | converts a timezone-agnostic timestamp to a timezone-aware timestamp in the provided timezone before rendering that timestamp in UTC                                                                                                                                                                                     |
| functions.format_number           | formats numbers using the specified number of decimal places                                                                                                                                                                                                                                                             |
| ~~functions.reverse~~            | ~~returns a reversed string~~ **Available in snowpark-python >= 1.2.0**                                                                                                                                                                                                                                           |
| ~~functions.explode~~            | ~~returns a new row for each element in the given array~~ **Available in snowpark-python >= 1.4.0**                                                                                                                                                                                                               |
| ~~functions.explode_outer~~      | ~~returns a new row for each element in the given array or map. Unlike explode, if the array/map is null or empty then null is producedThis~~<br />**Available in snowpark-python >= 1.4.0**<br />There is a breaking change as the explode_outer does not need the map argument anymore.                         |
| functions.arrays_zip              | returns a merged array of arrays                                                                                                                                                                                                                                                                                         |
| functions.array_sort              | sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.                                                                                                                                                              |
| functions.array_max               | returns the maximon value of the array.                                                                                                                                                                                                                                                                                  |
| functions.array_min               | returns the minimum value of the array.                                                                                                                                                                                                                                                                                  |
| ~~functions.array_distinct~~     | ~~removes duplicate values from the array.~~ **Available in snowpark-python >= 1.4.0**                                                                                                                                                                                                                           |
| ~~function.daydiff~~             | ~~this function returns the difference in days between two dates. This function will be direct equivalent of the spark datediff. You can simple replace spark datediff by daydiff~~<br />**Available in snowpark-python >= 1.4.0**                                                                                |
| ~~functions.date_add~~           | ~~returns the date that is n days days after~~<br />**Available in snowpark-python >= 1.4.0**                                                                                                                                                                                                                     |
| ~~functions.date_sub~~           | ~~returns the date that is n days before~~<br />**Available in snowpark-python >= 1.4.0**                                                                                                                                                                                                                         |
| ~~functions.regexp_extract~~     | ~~extract a specific group matched by a regex, from the specified string column.~~<br />**Available in snowpark-python >= 1.4.0**                                                                                                                                                                                 |
| functions.regexp_split            | splits a specific group matched by a regex, it is an extension of split wich supports a limit parameter.                                                                                                                                                                                                                 |
| ~~functions.asc~~                | ~~returns a sort expression based on the ascending order of the given column name.~~ **Available in snowpark-python >=1.1.0**                                                                                                                                                                                     |
| ~~functions.desc~~               | ~~returns a sort expression based on the descending order of the given column name.~~ **Available in snowpark-python >=1.1.0**                                                                                                                                                                                    |
| functions.flatten                 | creates a single array from an array of arrays                                                                                                                                                                                                                                                                           |
| functions.sort_array              | sorts the input array in ascending or descending order according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order                                                           |
| functions.map_values              | Returns an unordered array containing the values of the map.                                                                                                                                                                                                                                                             |
| ~~functions.struct~~             | ~~Returns an object built with the given columns~~<br />**Available in snowpark-python >= 1.4.0**                                                                                                                                                                                                                 |
| ~~functions.bround~~             | ~~This function receives a column with a number and rounds it to scale decimal places with HALF_EVEN round mode, often called as "Banker's rounding" . This means that if the number is at the same distance from an even or odd number, it will round to the even number.~~<br />Available in snowpark-python >= 1.4.0 |

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

| Name                              | Description                                                 |
| --------------------------------- | ----------------------------------------------------------- |
| utils.map_to_python_type          | maps from DataType to python type                           |
| utils.map_string_type_to_datatype | maps a type by name to a snowpark `DataType`              |
| utils.schema_str_to_schema        | maps an schema specified as an string to a `StructType()` |

## Jupyter Notebook support

A Jupyter extension has been created to allow integration in Jupyter notebooks. This extension implements a SQL magic, enabling users to run SQL commands within the Jupyter environment. This enhances the functionality of Jupyter notebooks and makes it easier for users to access and analyze their data using SQL. With this extension, data analysis becomes more streamlined, as users can execute SQL commands directly in the same environment where they are working on their notebooks.

To enable this extension just import the snowpark extensions module

```
import snowpark_extensions
```

After import a `%%sql` magic can be used to run queries. For example:

```
%%sql
select * from table1
```

Queries can use also use `Jinja2` syntax. For example:

If a previous cell you had something like:

```python
COL1=1
```

Then on following cells you can do:

```sql
%%sql
select * from tables where col={{COL1}}
```

You can give a name to the sql that you can use later for example:

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

```python
DataFrame.__rows_count = 1000
```

You can configure Jupyter to run some imports and initialization code at the start of a notebook by creating a file called `startup.ipy` in the `~/.ipython/profile_default/startup` directory.

Any code written in this file will be executed when you start a new Jupyter notebook.

An [example startup.ipy](https://github.com/MobilizeNet/snowpark-extensions-py/blob/main/startup.ipy) is provided

# Logging Extras

## Snowpark Tags

Bart, an Snowflake Evangelist from EMEA created this amazing utility.
We are just wrapping it here in the extensions. See his post for a great description
https://medium.com/snowflake/simple-tags-in-snowflake-snowpark-for-python-c5910749273

You can use in your snowpark procedures:

```
def process(session,...):
    @Tag()
    def foo(...):
        ....
    # or inside your code:
    def goo(...):
        # only queries from this context will be tagged
        with Tag(session, f"drop_in_do_it_too"):
            session.sql(f'''DROP TABLE IF EXISTS {to_table}''').collect()
```
