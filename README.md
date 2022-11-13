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

## Functions Extensions

| Name                         | Description                                                                         |
|------------------------------|-------------------------------------------------------------------------------------|
| functions.unix_timestamp     | returns the UNIX timestamp of current time.                                         |
| functions.from_unixtimestamp | can be used to convert UNIX time to Snowflake timestamp                             |
| functions.format_number      | formats numbers using the specified number of decimal places                        |
| functions.reverse            | returns a reversed string                                                           |
| functions.explode            | returns a new row for each element in the given array                               |
| functions.date_add           | returns the date that is n days days after                                          |
| functions.date_sub           | returns the date that is n days before                                              |

## Usage:

just import it at the top of your file and it will automatically extend your snowpark package
