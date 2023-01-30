


from snowflake.snowpark import functions as F
from snowflake.snowpark import context
from snowflake.snowpark.functions import call_builtin, col,lit, concat, coalesce, object_construct_keep_null
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
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark._internal.analyzer.unary_expression import Alias

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

    def format_number(col,d):
        col = _to_col_if_str(col,"format_number")
        return F.to_varchar(col,'999,999,999,999,999.' + '0'*d)

    def reverse(col):
        col = _to_col_if_str(col,"reverse")
        return F.call_builtin('reverse',col)

    def daydiff( col1: ColumnOrName, col2: ColumnOrName) -> Column:
        """Calculates the difference between two date, or timestamp columns based in days"""
        c1 = _to_col_if_str(col1, "daydiff")
        c2 = _to_col_if_str(col2, "daydiff")
        return F.call_builtin("datediff",lit("day"), c2,c1)

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

    F._sort_array_function = None
    def _sort_array(col:ColumnOrName,asc:ColumnOrLiteral=True):
        if not F._sort_array_function:
            session = context.get_active_session()
            current_database = session.get_current_database()
            function_name =_generate_prefix("_sort_array_helper")
            F._sort_array_function = f"{current_database}.public.{function_name}"
            session.sql(f"""
            create or replace temporary function {F._sort_array_function}(ARR ARRAY,ASC BOOLEAN) returns ARRAY
            language javascript as
            $$
            ARRLENGTH = ARR.length;
            // filter nulls
            ARR = ARR.filter(x => x !== null);
            if (ARR.length && ARR[0] instanceof Object)
            {{
                function sortFn() 
                {{
                    var sortByProps = Array.prototype.slice.call(arguments),
                        cmpFn = function(left, right, sortOrder) {{
                            var sortMultiplier = sortOrder === "asc" ? 1 : -1;
                            if (left > right) {{ return +1 * sortMultiplier;}}
                            if (left < right) {{ return -1 * sortMultiplier;}}
                            return 0;
                        }};
                    return function(sortLeft, sortRight) {{
                        // get value from object by complex key
                        var getValueByStr = function(obj, path) {{
                        var i, len;
                        //prepare keys
                        path = path.replace('[', '.');
                        path = path.replace(']', '');
                        path = path.split('.');
                        len = path.length;
                        for (i = 0; i < len; i++) {{
                        if (!obj || typeof obj !== 'object') {{ return obj;}}
                        obj = obj[path[i]];
                        }}
                return obj;
                }};
                return sortByProps.map(function(property) {{
                    return cmpFn(getValueByStr(sortLeft, property.prop), getValueByStr(sortRight, property.prop), property.sortOrder);
                }}).reduceRight(function(left, right) {{
                    return right || left;
            }});
        }};
        }}
        var props = Object.getOwnPropertyNames(ARR[0]);
        var sortKeys = [];
        for(var p of props)
        {{
            sortKeys.push({{prop:p,sortOrder:"asc"}});
        }}
        ARR.sort(sortFn(...sortKeys));
        }}
        else
            ARR.sort();
            var RES = new Array(ARRLENGTH-ARR.length).fill(null).concat(ARR);
            if (ASC) return RES; else return RES.reverse();
        $$;""").show()
        return call_builtin(F._sort_array_function,col,asc)


    F._array_sort_function = None
    def _array_sort(col:ColumnOrName):
        if not F._array_sort_function:
            session = context.get_active_session()
            current_database = session.get_current_database()
            function_name =_generate_prefix("_array_sort_helper")
            F._array_sort_function = f"{current_database}.public.{function_name}"
            session.sql(f"""
            create or replace temporary function {F._array_sort_function}(ARR ARRAY) returns ARRAY
            language javascript as
            $$
            ARRLENGTH = ARR.length;
            // filter nulls
            ARR = ARR.filter(x => x !== null);
            if (ARR.length && ARR[0] instanceof Object)
            {{
                function sortFn() 
                {{
                    var sortByProps = Array.prototype.slice.call(arguments),
                        cmpFn = function(left, right, sortOrder) {{
                            var sortMultiplier = sortOrder === "asc" ? 1 : -1;
                            if (left > right) {{ return +1 * sortMultiplier;}}
                            if (left < right) {{ return -1 * sortMultiplier;}}
                            return 0;
                        }};
                    return function(sortLeft, sortRight) {{
                        // get value from object by complex key
                        var getValueByStr = function(obj, path) {{
                        var i, len;
                        //prepare keys
                        path = path.replace('[', '.');
                        path = path.replace(']', '');
                        path = path.split('.');
                        len = path.length;
                        for (i = 0; i < len; i++) {{
                        if (!obj || typeof obj !== 'object') {{ return obj;}}
                        obj = obj[path[i]];
                        }}
                return obj;
                }};
                return sortByProps.map(function(property) {{
                    return cmpFn(getValueByStr(sortLeft, property.prop), getValueByStr(sortRight, property.prop), property.sortOrder);
                }}).reduceRight(function(left, right) {{
                    return right || left;
            }});
        }};
        }}
        var props = Object.getOwnPropertyNames(ARR[0]);
        var sortKeys = [];
        for(var p of props)
        {{
            sortKeys.push({{prop:p,sortOrder:"asc"}});
        }}
        ARR.sort(sortFn(...sortKeys));
        }}
        else
            ARR.sort();
        var RES = ARR.concat(new Array(ARRLENGTH-ARR.length).fill(null));
        return RES;
        $$;""").show()
        return call_builtin(F._array_sort_function,col)        
    F._array_max_function = None
    def _array_max(col:ColumnOrName):
        if not F._array_max_function:
            session = context.get_active_session()
            current_database = session.get_current_database()
            function_name =_generate_prefix("_array_max_function")
            F._array_max_function = f"{current_database}.public.{function_name}"
            session.sql(f"""
            create or replace temporary function {F._array_max_function}(ARR ARRAY) returns VARIANT
            language javascript as
            $$
            return Math.max(...ARR);
            $$
            """).show()
        return call_builtin(F._array_max_function,col)
    F._array_min_function = None
    def _array_min(col:ColumnOrName):
        if not F._array_min_function:
            session = context.get_active_session()
            current_database = session.get_current_database()
            function_name =_generate_prefix("_array_min_function")
            F._array_min_function = f"{current_database}.public.{function_name}"
            session.sql(f"""
            create or replace temporary function {F._array_min_function}(ARR ARRAY) returns VARIANT
            language javascript as
            $$
            return Math.min(...ARR);
            $$
            """).show()
        return call_builtin(F._array_min_function,col)

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

    def _bround(col: Column, scale: int = 0): 
        power = pow(F.lit(10), F.lit(scale))
        elevatedColumn = F.when(F.lit(0) == F.lit(scale), col).otherwise(col * power)
        columnFloor = F.floor(elevatedColumn)
        return F.when(
            elevatedColumn - columnFloor == F.lit(0.5)
            , F.when(columnFloor % F.lit(2) == F.lit(0), columnFloor).otherwise(columnFloor + F.lit(1))
        ).otherwise(F.round(elevatedColumn)) / F.when(F.lit(0) == F.lit(scale), F.lit(1)).otherwise(power)
    

    F.array = _array
    F.array_max = _array_max
    F.array_min = _array_min
    F.array_distinct = array_distinct
    F.regexp_extract = regexp_extract
    F.create_map = create_map
    F.format_number = format_number
    F.reverse = reverse
    F.daydiff = daydiff
    F.date_add = date_add
    F.date_sub = date_sub
    F.sort_array = _sort_array
    F.array_sort = _array_sort
    F.struct = _struct
    F.bround = _bround