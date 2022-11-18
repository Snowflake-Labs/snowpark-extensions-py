# snowpark-extensions-py

Snowpark by itself is a powerful library, but still some utility functions can always help.

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



## Functions Extensions

| Name                         | Description                                                                         |
|------------------------------|-------------------------------------------------------------------------------------|
| functions.array_sort         | sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array. |
| functions.unix_timestamp     | returns the UNIX timestamp of current time.                                         |
| functions.from_unixtimestamp | can be used to convert UNIX time to Snowflake timestamp                             |
| functions.format_number      | formats numbers using the specified number of decimal places                        |
| functions.reverse            | returns a reversed string                                                           |
| functions.explode            | returns a new row for each element in the given array                               |
| functions.date_add           | returns the date that is n days days after                                          |
| functions.date_sub           | returns the date that is n days before                                              |

## Usage:

just import it at the top of your file and it will automatically extend your snowpark package
