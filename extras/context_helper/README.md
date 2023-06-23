# Context Helper

A pattern that is useful for some user is leverage the context-manager or `with` approach which leads to very clean code. 

For more details read [python-with-statement](https://realpython.com/python-with-statement/).

So, the question is, can I do this in Snowpark. Yes, definite, you can and we provide a helper that can be used to do that. 

to use the helper just do:

```
from sfpark_contextmanager import SessionContextManager
```

and then you can do code snippets like:

```
from snowflake.snowpark import Session
from sfpark_contextmanager import SessionContextManager

cfg = json.loads(open(os.path.expanduser('~/credentials.json')).read())
builder_object = Session.builder.configs(cfg) 

with SessionContextManager(builder_object) as session:
    session.sql("select 1 A").show()
    session.sql("select malo").collect()

print("Done")
```

What the code then does, is that:

1. it will open the session at the start of the with block
2. it will start a transaction
3. it will execute the each statement
4. if an statement fails it will catch the exception report it. It will use use logging in case you can to leverage our [snowpark logging](https://docs.snowflake.com/developer-guide/logging-tracing/logging)

# Using in a Snowpark Procedure

Using it in a procedure is pretty easy. Upload it to an stage. 

## First upload the `sfpark_contextmanager.py` file to your stage.

### Uploading with SnowCLI

If you have SnowCLI you can install it from the command line like:

`snowsql -q "PUT file://sfpark_contextmanager.py @mystage AUTO_COMPRESS=FALSE OVERWRITE=TRUE"`

### Uploading with SnowSight

see https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui


## Update your procedure definitions


Now in your stored procedure. You need to add the `sfpark_contextmanager.py` as well as the `.whl` files in the IMPORTS section.

For example:

```python
create or replace procedure EXAMPLE_PROC1(arg1 VARCHAR1, arg2 VARCHAR2)
returns variant
language python
volatile
runtime_version = '3.8'
imports=('@MYSTAGE/sfpark_contextmanager.py',)
packages = ( ... ) -- just put any packages you need there
handler = 'your_handler'
as
$$
import sfpark_contextmanager
def your_handler(arg1, arg2):

    # ... rest of the code
    with SessionContextManager(builder_object) as session:
      session.sql("select 1 A").show()
      session.sql("select malo").collect()
    return {}
$$
```
