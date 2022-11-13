from snowflake.snowpark import functions as F
from snowflake.snowpark import DataFrame

def unix_timestamp(col):
	return call_builtin("DATE_PART","epoch_second",col)

def from_unixtime(col):
    return F.to_timestamp(col).alias('ts')

def format_number(col,d):
    return F.to_varchar(col,'999,999,999,999,999.' + '0'*d)

def reverse(col):
    return F.call_builtin('reverse',col)

def date_add(col,num_of_days):
	return dateadd(lit('day'),col,num_of_days)

def date_sub(col,num_of_days):
	return dateadd(lit('day'),col,-1 * num_of_days)

# EXPLODE HELPERS
class Explode:
    def __init__(self,expr):
        self.expr = expr

def explode(expr):
    return Explode(expr)

F.explode = explode

DataFrame.oldwithColumn = DataFrame.withColumn
def withColumnExtended(self,colname,expr):
    if isinstance(expr, Explode):
        return self.join_table_function('flatten',date_range_udf(col("epoch_min"), col("epoch_max"))).drop(["SEQ","KEY","PATH","INDEX","THIS"]).rename("VALUE",colname)
    else:
        self.oldwithColumn(colname,expr)
DataFrame.withColumn = withColumnExtended


F.unix_timestamp = unix_timestamp
F.from_unixtime = from_unixtime
F.format_number = format_number
F.reverse = reverse
F.date_data = date_add
F.date_sub = date_sub

import snowpark_extensions.session_builder_extensions