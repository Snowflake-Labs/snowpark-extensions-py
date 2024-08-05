# SQL Alchemy Integration with Snowpark

SQL alchemy is a package that is heavily used in many frameworks and applications.

I was then tempted to combine it with Snowpark. It is posible. Well it is. It required a couple of tricks but I was able to get this working.

You can just import this file in your streamlit / snowpark procedure or snowflake notebook.

and then do:

```
from sqlalchemy_snowpark import create_sqlalchemy_engine
engine = create_sqlalchemy_engine(session)
```
