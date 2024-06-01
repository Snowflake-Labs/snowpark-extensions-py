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
# load wheel and add to path 
wheel_loader.load('pypiexample1-0.1.0-py3-none-any.whl')
wheel_loader.load('pypiexample2-0.1.0-py3-none-any.whl')

def your_handler(arg1, arg2):
    from pypiexample1.some_module import my_function1
    from pypiexample2.some_other_module import my_function2
  
    return my_function2(my_function1(arg1, arg2))
$$
```

Notice the `wheel_loader` import at the top and this file, and the calls of `wheel_loader.load`. Those are needed to ensure that your packages are loaded.

> UPDATE: having to properly type your wheel names was too cumbesome. If you want to control explicitly how your wheels are loaded you can still use the `wheel_load.load` method but now you can also just wheel_loader.


# wheel_loader.add_wheels
To make the usage of the wheel_loader even easier, you can also try a more simplified approach using `wheel_loader.add_wheels`.

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
# add any .whl file added to your imports 
wheel_loader.add_wheels()

def your_handler(arg1, arg2):
    from pypiexample1.some_module import my_function1
    from pypiexample2.some_other_module import my_function2
    
    return my_function2(my_function1(arg1, arg2))
$$
```

That rest is just python bliss :)

This snippet was developed by [James Weakley](https://medium.com/@jamesweakley) in a [Medium post](https://medium.com/snowflake/running-pip-packages-in-snowflake-d43581a67439), check it out for more details.
