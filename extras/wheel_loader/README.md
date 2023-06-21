# Wheel Loader

Sometimes you need to use some packages that are not available in SF Anaconda channel.

A simple utility file can be used in those cases.

To use it follow these steps:

## First upload the `wheel_loader.py` file to your stage.

### Uploading with SnowCLI

If you have SnowCLI you can install it from the command line like:

`snowsql -q "PUT file://wheel_loader.py @mystage AUTO_COMPRESS=FALSE OVERWRITE=TRUE"`

### Uploading with SnowSight

see https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui

## Next upload the `.whl` files to your stage


Now in your stored procedure. You need to add the `wheel_loader.py` as well as the `.whl` files in the IMPORTS section.

For example:

```python
create or replace function EXAMPLE_UDF(arg1 VARCHAR1, arg2 VARCHAR2)
returns variant
language python
volatile
runtime_version = '3.8'
imports=('@MYSTAGE/wheel_loader.py',
         '@MYSTAGE/pypiexample1-0.1.0-py3-none-any.whl',
         '@MYSTAGE/pypiexample2-0.1.0-py3-none-any.whl'
)
packages = ( ... ) -- just put any packages you need there
handler = 'your_handler'
as
$$
import wheel_loader
def your_handler(arg1, arg2):
    wheel_loader.load('pypiexample1-0.1.0-py3-none-any.whl')
    wheel_loader.load('pypiexample2-0.1.0-py3-none-any.whl')
    # ... rest of the code
    return {}
$$
```

Notice the `wheel_loader` import at the top and this file, and the calls of `wheel_loader.load`. Those are needed to ensure that your packages are loaded. 

That rest is just python bliss :)

This snippet was developed by [James Weakley](https://medium.com/@jamesweakley) in a [Medium post](https://medium.com/snowflake/running-pip-packages-in-snowflake-d43581a67439), check it out for more details.
