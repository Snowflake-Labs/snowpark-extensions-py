import logging
from .transform import SFTransform, find_field
from snowflake.snowpark import DataFrame as DynamicFrame
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name_without_upper_casing
from snowflake.snowpark.functions import lit, try_cast, split, iff, typeof, col, object_construct, cast, coalesce, builtin
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import snow_type_to_dtype_str
import logging
from snowflake.snowpark._internal.utils import quote_name
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted


from sfjobs.transforms.apply_mapping  import ApplyMapping
from snowflake.snowpark.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, BooleanType, DateType, TimestampType, VariantType, BinaryType
try_parse_json = builtin("try_parse_json")

class ResolveChoice(SFTransform):
    def __call__(self, frame, specs=None, choice="", database=None, table_name=None, transformation_ctx="", info="",ignore_case=True, **kwargs):
        df = frame
        if transformation_ctx:
            logging.info(f"CTX: {transformation_ctx}")
        if len(kwargs):
            logging.warning(f"ignored KWARGS: {kwargs}")
        for col_name, operation in specs:
            if operation.startswith('cast:'):
                # Cast the column to the specified type
                dtype = operation.split(':')[1]
                current_field = find_field(col_name, df, ignore_case=ignore_case)
                col_name = current_field.name
                current_type = snow_type_to_dtype_str(current_field.datatype)
                if current_type != dtype:
                    df = df.withColumn(col_name, try_cast(cast(col(col_name),"string"), dtype))
            elif operation.startswith('project:'):
                # Project the column with the specified type
                dtype = operation.split(':')[1].upper()
                valid_dtypes = ['NULL_VALUE', 'NULL', 'BOOLEAN', 'STRING', 'INTEGER', 'DOUBLE', 'VARCHAR', 'ARRAY','OBJECT']
                if dtype == 'LONG':
                    dtype = 'INTEGER'
                current_field = find_field(col_name, df, ignore_case=ignore_case)
                col_name = current_field.name
                col_expr = coalesce(try_parse_json(df[col_name]),df[col_name].cast("variant"))
                if not dtype in valid_dtypes:
                    raise ValueError(f"Invalid data type: {dtype}. Valid values are {valid_dtypes}")
                df = df.withColumn(col_name, iff(typeof(col_expr) == lit(dtype), cast(cast(df[col_name],"string"), dtype),lit(None)))
            elif operation == 'make_cols': 
                current_field = find_field(col_name, df, ignore_case=ignore_case)
                col_name = current_field.name
                col_expr = coalesce(try_parse_json(df[col_name]),df[col_name].cast("variant"))
                current_types = [x[0] for x in df.select(typeof(col_expr)).distinct().collect()]
                for t in current_types:               
                    df = df.withColumn(f"{unquote_if_quoted(col_name)}_{t}", iff(typeof(col_expr) == lit(t), df[col_name].cast(current_field.datatype), lit(None)))
                df = df.drop(col_name)
            elif operation == 'make_struct':        
                struct_elements = []
                current_field = find_field(col_name, df, ignore_case=ignore_case)
                col_expr = coalesce(try_parse_json(df[col_name]),df[col_name].cast("variant"))
                current_types = [x[0] for x in df.select(typeof(col_expr)).distinct().collect()]
                for t in current_types:
                    struct_elements.append(lit(t))
                    struct_elements.append(iff(typeof(col_expr) == lit(t), df[col_name], lit(None)))
                df = df.withColumn(col_name, object_construct(*struct_elements))
            else:
                raise ValueError(f"Unsupported operation: {operation}")
        return df

