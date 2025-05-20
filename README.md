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

| Name                        | Description                                                                                                 |
| --------------------------- | ----------------------------------------------------------------------------------------------------------- |
| SessionBuilder.from_snowsql | can read the information from the snowsql config file by default at ~/snowsql/config or at a given location |
| SessionBuilder.env          | reads settings from SNOW_xxx or SNOWSQL_xxx variables                                                       |

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
> [connections]
> [connections.default]
> account = "myaccount"
> user = "user1"
> password = 'xxxxx'
> role = "user_role"
> database = "demodb"
> schema = "public"
> warehouse = "load_wh"
>
> [connections.snowpark]
> account = "myaccount"
> user = "user2"
> password = 'yyyyy'
> role = "user_role"
> database = "demodb"
> schema = "public"
> warehouse = "load_wh"
> authenticator = "externalbrowser"
>
> ```

The `Session.app_name`  or `Session.append_query_tag(f"execution_id={some_id}") ` can used to setup a query_tag like `APPNAME=tag;execution_id=guid` which can then be used to track job actions with a query like

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

## DataFrame Extensions

| Name                       | Description                                                                                                                                                            |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DataFrameReader.ingest_csv | provides an additional reader that performs a data loading for csv which might have data with data inconsistencies |
| DataFrame.map              | provides an equivalent for the map function for example `df.map(func,input_types=[StringType(),StringType()],output_types=[StringType(),IntegerType()],to_row=True)` |
| DataFrame.simple_map       | if a simple lambda like `lambda x: x.col1 + x.col2` is used this functions can be used like `df.simple_map(lambda x: x.col1 + x.col2)`                             |
| DataFrame.replace          | extends replace to allow using a regex                                                                                                                                 |
| DataFrame.groupBy.pivot    | extends the snowpark groupby to add a pivot operator                                                                                                                   |
| DataFrame.stack            | This is an operator similar to the unpivot operator                                                                                                                    |
| DataFrame.transform        | This method is used mostly the provide a chaining syntax. For example:`df.transform(func1).transform(func2).show()`                                                  |
| ~~DataFrame.unionByName~~ | Deprecated, this is now available in snowpark~~. This extensions extends the unionByName to include support for**allowMissingValues**~~                         |

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

### transform

```
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf

# Initialize session
session = Session.builder.app_name("TransformExample").getOrCreate()

# Sample DataFrame
data = [(1,), (2,), (3,), (4,)]
df = session.createDataFrame(data, ["number"])

# Define the first function to add 1
def add_one(x:int)->int:
    return x + 1

# Define the second function to multiply by 2
def multiply_by_two(x:int)->int:
    return x * 2

# Register UDFs
add_one_udf = udf(add_one)
multiply_by_two_udf = udf(multiply_by_two)

# Apply the transformations using transform
df_transformed = df.withColumn(
    "transformed_number", 
    multiply_by_two_udf(add_one_udf(col("number")))
)

# Show the result
df_transformed.show()

```

Will show this output:

```
-----------------------------------  
|"NUMBER"  |"TRANSFORMED_NUMBER"  |  
-----------------------------------  
|4         |10                    |  
|1         |4                     |  
|3         |8                     |  
|2         |6                     |  
-----------------------------------
```

## Functions Extensions

| Name                           | Description                                                                                                                                                          |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|                                |                                                                                                                                                                      |
| functions.to_utc_timestamp_ext | converts a timezone-agnostic timestamp to a timezone-aware timestamp in the provided timezone before rendering that timestamp in UTC it supports timezone names like |
| functions.format_number        | formats numbers using the specified number of decimal places                                                                                                         |
| functions.arrays_zip           | returns a merged array of arrays                                                                                                                                     |
| functions.regexp_split         | splits a specific group matched by a regex, it is an extension of split wich supports a limit parameter.                                                             |
| functions.flatten              | creates a single array from an array of arrays                                                                                                                       |
| functions.map_values           | Returns an unordered array containing the values of the map.                                                                                                         |

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

## Notebook support

A Jupyter extension has been created to allow integration in Jupyter notebooks. This extension implements a SQL magic, enabling users to run SQL commands within the Jupyter environment.

This enhances the functionality of Jupyter notebooks and makes it easier for users to access and analyze their data using SQL. With this extension, data analysis becomes more streamlined, as users can execute SQL commands directly in the same environment where they are working on their notebooks.

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
