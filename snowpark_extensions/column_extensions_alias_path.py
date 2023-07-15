import json
from snowflake.snowpark import Session
#from pathlib import Path
# read credentials from ~/credentials.json
#session = Session.builder.configs(json.load(open(str(Path.home().joinpath("credentials.json"))))).create()

from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *

def extend_column_alias_and_cast():
    from snowflake.snowpark import Column
    from snowflake.snowpark._internal.analyzer.unary_expression import Alias
    Column.__alias = Column.alias
    
    def ext_cast(self, to) -> "Column":
        if isinstance(self._expression,Alias):
                return col(self._expression.children[0])._cast(to, False).__alias(self._expression.name)
        else:
             return self._cast(to, False)
    Column.cast = ext_cast
    def ext_alias(self, alias: str,metadata:dict=None) -> "Column":
        if metadata and "maxlength" in metadata:
           length = metadata['maxlength']
           return sql_expr(f"cast(left({self._expression.sql},{length}) as string({length}))").__alias(alias)
        else:
            return self.__alias(alias)
    Column.alias = ext_alias

# this method is called to "extend" the Snowpark Column implementation
extend_column_alias_and_cast()
# after this call the column will allow

# df.myColumn.alias("COL1", metadata={"maxlength":255})
# or it will also allow applying a cast to an alias

schema = StructType([StructField("c0",StringType()),StructField("c1",StringType()),StructField("c2",StringType())])
df = session.create_dataframe(data=[("Hola Mundo","Hola Mundo","Hola Mundo")],schema=schema)
df = df.select(col("c0").alias("c0").cast(StringType()),
               col("c1").alias("c1",metadata={'maxlength':2}),
               col("c2").alias("c2",metadata={'maxlength':4}))
df.show()
# ----------------------------------
# |"C0"        |"C1"  |"C2"        |
# ----------------------------------
# |Hola Mundo  |Ho    |Hola Mundo  |
# ----------------------------------
df.write.saveAsTable("temp1",table_type="temp")
session.sql("describe table temp1").show()


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------
# |"name"  |"type"             |"kind"  |"null?"  |"default"  |"primary key"  |"unique key"  |"check"  |"expression"  |"comment"  |"policy name"  |"resource group"  |
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------
# |C0      |VARCHAR(16777216)  |COLUMN  |Y        |NULL       |N              |N             |NULL     |NULL          |NULL       |NULL           |NULL              |
# |C1      |VARCHAR(2)         |COLUMN  |Y        |NULL       |N              |N             |NULL     |NULL          |NULL       |NULL           |NULL              |
# |C2      |VARCHAR(4)         |COLUMN  |Y        |NULL       |N              |N             |NULL     |NULL          |NULL       |NULL           |NULL              |
# ----------------------------------------