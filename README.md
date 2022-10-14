# snowpark-extensions-py

Snowpark by itself is a powerful library, but still some utility functions can always help.

## Currently provided extensions:

| Name                         | Description                                                                         |
| functions.unix_timestamp     | returns the UNIX timestamp of current time.                                         |
| functions.from_unixtimestamp | can be used to convert UNIX time to Snowflake timestamp                             |
| functions.format_number      | formats numbers using the specified number of decimal places                        |
| functions.reverse            | returns a reversed string                                                           |
| functions.explode            | returns a new row for each element in the given array                               |

## Usage:

just import it at the top of your file and it will automatically extend your snowpark package
