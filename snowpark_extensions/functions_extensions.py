


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
        return F.to_timestamp(col).alias('ts')

    def format_number(col,d):
        return F.to_varchar(col,'999,999,999,999,999.' + '0'*d)

    def reverse(col):
        return F.call_builtin('reverse',col)

    def date_add(col,num_of_days):
        return dateadd(lit('day'),col,num_of_days)

    def date_sub(col,num_of_days):
        return dateadd(lit('day'),col,-1 * num_of_days)

    def create_map(*col_names):
        """
        Usage:
        res = df.select(create_map('name', 'age').alias("map")).collect()
        """
        from snowflake.snowpark.functions import col,lit, object_construct
        col_list = []
        if isinstance(col_names, tuple):
            col_names = list(col_names[0])
        if isinstance(c, str):
            col_list.append(lit(c))
            col_list.append(col(c))
        else:
            col_list.append(lit(str(c._expression).replace("\"",'')))
            col_list.append(c)
        return object_construct(*col_list)



    array_sort_udf=None
    def array_sort(array, asc: bool=True):
        global array_sort_udf
        if not array_sort_udf:
            def _array_sort(array:list, asc: bool)->list:
                def compare(item1, item2=None):
                    if item1 is None:
                        return 1
                    elif item2 is None:
                        return -1
                    elif item1 < item2:
                        return -1
                    elif item1 > item2:
                        return 1
                    else:
                        return 0
                array.sort(key=compare,reverse=not asc)
                return array
            array_sort_udf = F.udf(_array_sort,return_type=ArrayType(),input_types=[ArrayType(),BooleanType()],name="array_sort",is_permanent=False,replace=True)
        return array_sort_udf(array, F.lit(asc))

    F.regexp_extract = regexp_extract
    F.array_sort = array_sort
    F.create_map = create_map
    F.unix_timestamp = unix_timestamp
    F.from_unixtime = from_unixtime
    F.format_number = format_number
    F.reverse = reverse
    F.date_data = date_add
    F.date_sub = date_sub