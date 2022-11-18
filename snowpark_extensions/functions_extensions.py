


from snowflake.snowpark import functions as F
from snowflake.snowpark import DataFrame

if not hasattr(F,"___extended"):
    F.___extended = True
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

    def array_sort(array:list)->list:
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
        array.sort(key=compare)
        return array

    array_sort_udf=None
    def array_sort(array):
        if not array_sort_udf:
            array_sort_udf = udf(array_sort,return_type=ArrayType(),input_types=[ArrayType()],name="array_sort",is_permanent=False,replace=True)
        array_sort_udf(array)

    F.array_sort = array_sort
    F.create_map = create_map
    F.unix_timestamp = unix_timestamp
    F.from_unixtime = from_unixtime
    F.format_number = format_number
    F.reverse = reverse
    F.date_data = date_add
    F.date_sub = date_sub