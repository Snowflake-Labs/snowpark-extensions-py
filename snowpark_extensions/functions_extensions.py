


from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import call_builtin, lit, concat, coalesce
from snowflake.snowpark import DataFrame, Column
from snowflake.snowpark.types import ArrayType, BooleanType
from snowflake.snowpark._internal.type_utils import (
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
)
from snowflake.snowpark.column import _to_col_if_str, _to_col_if_lit


if not hasattr(F,"___extended"):
    F.___extended = True

    def pairwise(iterable):
        while len(iterable):
            a = iterable.pop(0)
            if len(iterable):
                b = iterable.pop(0)
            else:
                b = None
            yield (a,b)
    
    def flatten_col_list(obj):
        if isinstance(obj, str) or isinstance(obj, Column):
            return [obj]
        elif hasattr(obj, '__iter__'):
            acc = []
            for innerObj in obj:
                acc = acc + flatten_col_list(innerObj)
            return acc


    def regexp_extract(value:ColumnOrLiteralStr,regexp:ColumnOrLiteralStr,idx:int) -> Column:
        """
        Extract a specific group matched by a regex, from the specified string column. 
        If the regex did not match, or the specified group did not match, 
        an empty string is returned.        
        """
        value = _to_col_if_str(value,"regexp_extract")
        regexp = _to_col_if_lit(regexp,"regexp_extract")
        idx = _to_col_if_lit(idx,"regexp_extract")
        # we add .* to the expression if needed
        return coalesce(call_builtin('regexp_substr',value,regexp,lit(1),lit(1),lit('e'),idx),lit(''))

    def unix_timestamp(col):
        return call_builtin("DATE_PART","epoch_second",col)

    def from_unixtime(col):
        col = _to_col_if_str(col,"from_unixtime")
        return F.to_timestamp(col).alias('ts')

    def format_number(col,d):
        col = _to_col_if_str(col,"format_number")
        return F.to_varchar(col,'999,999,999,999,999.' + '0'*d)

    def reverse(col):
        col = _to_col_if_str(col,"reverse")
        return F.call_builtin('reverse',col)

    def date_add(col,num_of_days):
        col = _to_col_if_str(col,"date_add")
        num_of_days=_to_col_if_str_or_int(num_of_days)
        return dateadd(lit('day'),col,num_of_days)

    def date_sub(col,num_of_days):
        col = _to_col_if_str(col,"date_sub")
        num_of_days=_to_col_if_str_or_int(num_of_days)
        return dateadd(lit('day'),col,-1 * num_of_days)

    def create_map(*col_names):
        """
        Usage:
        res = df.select(create_map('name', 'age').alias("map")).collect()
        """
        from snowflake.snowpark.functions import col,lit, object_construct
        col_list = []
        # flatten any iterables, to process them in pairs
        col_names = flatten_col_list(col_names)
        for name, value in pairwise(col_names):
            if isinstance(name, str):
                col_list.append(lit(name))
            else:
                col_list.append(name)
            col_list.append(value)
        return object_construct(*col_list)

    def array_distinct(col):
        col = _to_col_if_str(col,"array_distinct")
        return F.call_builtin('array_distinct',col)


    def _array(*cols):
        return F.array_construct(*cols)

    F.array = _array
    F.array_distinct = array_distinct
    F.regexp_extract = regexp_extract
    F.create_map = create_map
    F.unix_timestamp = unix_timestamp
    F.from_unixtime = from_unixtime
    F.format_number = format_number
    F.reverse = reverse
    F.date_add = date_add
    F.date_sub = date_sub
    F.asc  = lambda col: _to_col_if_str(col, "asc").asc()
    F.desc = lambda col: _to_col_if_str(col, "desc").desc()
    F.asc_nulls_first = lambda col: _to_col_if_str(col, "asc_nulls_first").asc()
    F.desc_nulls_first = lambda col: _to_col_if_str(col, "desc_nulls_first").asc()