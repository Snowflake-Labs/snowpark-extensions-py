


from snowflake.snowpark import functions as F
from snowflake.snowpark import context
from snowflake.snowpark.functions import call_builtin, col,lit, concat, coalesce, object_construct_keep_null, table_function, udf
from snowflake.snowpark import DataFrame, Column
from snowflake.snowpark.types import ArrayType, BooleanType
from snowflake.snowpark._internal.type_utils import (
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
)
from snowflake.snowpark.column import _to_col_if_str, _to_col_if_lit, _to_col_if_str_or_int
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
import re

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

    def format_number(col,d):
        col = _to_col_if_str(col,"format_number")
        return F.to_varchar(col,'999,999,999,999,999.' + '0'*d)

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
            col_list.append(_to_col_if_str(name,"create_map"))
            col_list.append(value)
        return object_construct(*col_list)
    
    def _array(*cols):
        return F.array_construct(*cols)

    def _struct(*cols):
        new_cols = []
        for c in flatten_col_list(cols):
            if isinstance(c, str):
                new_cols.append(lit(c))
            else:
                name = c._expression.name
                name = name[1:] if name.startswith('"') else name
                name = name[:-1] if name.endswith('"') else name
                new_cols.append(lit(name))
            c = _to_col_if_str(c, "struct")
            if isinstance(c, Column) and isinstance(c._expression,Alias):
                new_cols.append(col(c._expression.children[0])) 
            else:
                new_cols.append(c)
        return object_construct_keep_null(*new_cols)

    F._array_flatten_udf = None
    def _array_flatten(array):
        if not F._array_flatten_udf:
            @udf
            def _array_flatten(array_in:list) -> list:
                flat_list = []
                for sublist in array_in:
                    if type(sublist) == list:
                        flat_list.extend(sublist)
                    else:
                        flat_list.append(sublist)              
                return flat_list
            F._array_flatten_udf = _array_flatten
        array = _to_col_if_str(array, "array_flatten")
        return F._array_flatten_udf(array)

    F._array_zip_udfs = {}

    def build_array_zip_ddl(nargs:int):
        function_name = _generate_prefix(f"array_zip_{nargs}")
        args          = ",".join([f"list{x} ARRAY" for x in range(1,nargs+1)])
        args_names    = ",".join([f"list{x}"       for x in range(1,nargs+1)])
        return function_name,f"""
CREATE OR REPLACE TEMPORARY FUNCTION {function_name}({args})
returns ARRAY language python runtime_version = '3.8'
handler = 'zip_list'
as
$$
def zip_list({args_names}):
    return list(zip({args_names}))
$$;"""

    def _arrays_zip(*lists):
        nargs = len(lists)
        if nargs < 2:
            raise Exception("At least two list are needed for array_zip")
        if not str(nargs) in F._array_zip_udfs:
            try:
                function_name, udf_ddl = build_array_zip_ddl(nargs)
                context.get_active_session().sql(udf_ddl).show()
                F._array_zip_udfs[str(nargs)] = function_name
            except Exception as e:
                raise Exception(f"Could not register support udf for array_zip. Error: {e}")
        list_cols = [_to_col_if_str(x, "array_zip") for x in lists]
        return F.call_builtin(F._array_zip_udfs[str(nargs)],*list_cols)

    def _bround(col: Column, scale: int = 0): 
        power = pow(F.lit(10), F.lit(scale))
        elevatedColumn = F.when(F.lit(0) == F.lit(scale), col).otherwise(col * power)
        columnFloor = F.floor(elevatedColumn)
        return F.when(
            elevatedColumn - columnFloor == F.lit(0.5)
            , F.when(columnFloor % F.lit(2) == F.lit(0), columnFloor).otherwise(columnFloor + F.lit(1))
        ).otherwise(F.round(elevatedColumn)) / F.when(F.lit(0) == F.lit(scale), F.lit(1)).otherwise(power)
    
    def has_special_char(string):
        pattern = '[^A-Za-z0-9]+'
        result = re.search(pattern, string)
        return bool(result)

    def is_not_a_regex(pattern):
        return not has_special_char(pattern)

    F._split_regex_udf = None
    def _regexp_split(value:ColumnOrName, pattern:ColumnOrLiteralStr, limit:int = -1):  
        value = _to_col_if_str(value,"split_regex")                
        pattern_col = pattern        
        if isinstance(pattern, str):
            pattern_col = lit(pattern)        
        if limit < 0 and isinstance(pattern, str) and is_not_a_regex(pattern):
            return F.split(value, pattern_col)  
                    
        session = context.get_active_session()
        current_database = session.get_current_database() 
        function_name =_generate_prefix("_regex_split_helper")           
        F._split_regex_udf = f"{current_database}.public.{function_name}"

        session.sql(f"""CREATE OR REPLACE FUNCTION {F._split_regex_udf} (input String, regex String, limit INT)
RETURNS ARRAY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
PACKAGES = ('com.snowflake:snowpark:latest')
HANDLER = 'MyJavaClass.regex_split_run' 
AS
$$
import java.util.regex.Pattern;
public class MyJavaClass {{
    public String[] regex_split_run(String input,String regex, int limit) {{
        Pattern pattern = Pattern.compile(regex);
        return pattern.split(input, limit);
    }}}}$$;""").show()
        return call_builtin(F._split_regex_udf, value, pattern_col, limit)

    F._map_values_udf = None
    def _map_values(col:ColumnOrName):
        col = _to_col_if_str(col,"map_values")
        if not F._map_values_udf:
            @udf(replace=True,is_permanent=False)
            def map_values(obj:dict)->list:
                return list(obj.values())
            F._map_values_udf = map_values
        return F._map_values_udf(col)

    timezoneMap = {
        'EST': '-05:00',
        'HST': '-10:00',
        'MST': '-07:00',
        'ACT': 'Australia/Darwin',
        'AET': 'Australia/Sydney',
        'AGT': 'America/Argentina/Buenos_Aires',
        'ART': 'Africa/Cairo',
        'AST': 'America/Anchorage',
        'BET': 'America/Sao_Paulo',
        'BST': 'Asia/Dhaka',
        'CAT': 'Africa/Harare',
        'CNT': 'America/St_Johns',
        'CST': 'America/Chicago',
        'CTT': 'Asia/Shanghai',
        'EAT': 'Africa/Addis_Ababa',
        'ECT': 'Europe/Paris',
        'IET': 'America/Indiana/Indianapolis',
        'IST': 'Asia/Kolkata',
        'JST': 'Asia/Tokyo',
        'MIT': 'Pacific/Apia',
        'NET': 'Asia/Yerevan',
        'NST': 'Pacific/Auckland',
        'PLT': 'Asia/Karachi',
        'PNT': 'America/Phoenix',
        'PRT': 'America/Puerto_Rico',
        'PST': 'America/Los_Angeles',
        'SST': 'Pacific/Guadalcanal',
        'VST': 'Asia/Ho_Chi_Minh'
    }
    def map_timestamp(tz_col):
         from functools import reduce
         when_exprs = F
         for tz, offset in timezoneMap.items():
             when_exprs = when_exprs.when( tz_col == F.lit(tz), F.lit(offset))
         return when_exprs.otherwise(tz_col)
    def to_utc_timestamp_ext(timestamp:ColumnOrName, tz:ColumnOrLiteral):
        """
        The function converts a timezone-agnostic timestamp to a timezone-aware 
        timestamp in the provided timezone before rendering that timestamp in UTC.
        The function add supports for a majority of timezone abbreviations         

        Arguments:
            timestamp: column or Name
            tz: A String with the time zone ID
        """
        if isinstance(tz, str):
            tz = timezoneMap.get(tz, tz)
            tz = F.lit(tz)
        elif isinstance(tz,Column):
            tz = map_timestamp(tz)
        timestamp = _to_col_if_str(timestamp,"to_utc_timestamp")
        return F.to_utc_timestamp(timestamp,tz) 



    F.array          = _array
    F.array_flatten  = _array_flatten
    F.arrays_zip     = _arrays_zip
    F.create_map     = create_map
    F.format_number  = format_number
    F.flatten        = _array_flatten
    F.map_values     = _map_values
    F.regexp_split   = _regexp_split
    F.to_utc_timestamp_ext = to_utc_timestamp_ext