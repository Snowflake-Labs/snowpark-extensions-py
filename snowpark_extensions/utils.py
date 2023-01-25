from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    GeographyType,
    LongType,
    MapType,
    StringType,
    StructType,
    StructField,
    TimestampType,
    TimeType,
    VariantType,
    FloatType
)
import datetime
import decimal
# based on https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing.html#label-sql-python-data-type-mappings

def map_to_python_type_str(dtype:DataType):
    if isinstance(dtype, ArrayType):
        return "list"
    elif isinstance(dtype, BinaryType):
        return "bytes"
    elif isinstance(dtype, BooleanType):
        return "bool"
    elif isinstance(dtype, DateType):
        return "datetime.date"
    elif isinstance(dtype, DecimalType):
        return "int" if dtype.scale == 0 else "decimal.Decimal"
    elif isinstance(dtype, FloatType):
        return "float"
    elif isinstance(dtype, DoubleType):
        return "float"
    elif isinstance(dtype, GeographyType):
        return "dict"
    elif isinstance(dtype, LongType):
        return "int"
    elif isinstance(dtype, MapType):
        return "dict"
    elif isinstance(dtype, StringType):
        return "str"
    elif isinstance(dtype, StructType):
        return "dict"
    elif isinstance(dtype, TimestampType):
        return "datetime.datetime"
    elif isinstance(dtype, TimeType):
        return "datetime.time"
    elif isinstance(dtype, VariantType):
        return "dict"

def map_to_python_type(dtype:DataType):
    if isinstance(dtype, ArrayType):
        return list
    elif isinstance(dtype, BinaryType):
        return bytes
    elif isinstance(dtype, BooleanType):
        return bool
    elif isinstance(dtype, DateType):
        return datetime.date
    elif isinstance(dtype, DecimalType):
        return int if dtype.scale == 0 else decimal.Decimal
    elif isinstance(dtype, FloatType):
        return float
    elif isinstance(dtype, DoubleType):
        return float
    elif isinstance(dtype, GeographyType):
        return dict
    elif isinstance(dtype, LongType):
        return int
    elif isinstance(dtype, MapType):
        return dict
    elif isinstance(dtype, StringType):
        return str
    elif isinstance(dtype, StructType):
        return dict
    elif isinstance(dtype, TimestampType):
        return datetime.datetime
    elif isinstance(dtype, TimeType):
        return datetime.time
    elif isinstance(dtype, VariantType):
        return dict


def map_python_type_to_datatype(type):
    if isinstance(type, list):
        return ArrayType
    elif isinstance(type, bytes):
        return BinaryType
    elif isinstance(type, bool):
        return BooleanType
    elif isinstance(type, datetime.date):
        return DateType
    elif isinstance(type, int):
        return LongType
    elif isinstance(type, float):
        return FloatType
    elif isinstance(type, decimal.Decimal):
        return DecimalType
    elif isinstance(type, dict):
        return MapType
    elif isinstance(type, str):
        return StringType
    elif isinstance(type, datetime.datetime):
        return TimestampType
    elif isinstance(type, datetime.time):
        return TimeType
    else:
        return VariantType

def map_string_type_to_datatype(type):
    type = type.lower()
    if type == "list":
        return ArrayType()
    elif type=="bytes":
        return BinaryType()
    elif type == "bool" or type == "boolean":
        return BooleanType()
    elif type == "date":
        return DateType()
    elif type == "int" or type == "long":
        return LongType()
    elif type == "float":
        return FloatType()
    elif type == "double":
        return DoubleType()
    elif type == "decimal":
        return DecimalType()
    elif type == "dict" or type == "struct":
        return MapType()
    elif type == "str" or type == "string" or type == "text":
        return StringType()
    elif type == "timestamp":
        return TimestampType()
    elif type == "time":
        return TimeType()
    else:
        return VariantType()

def schema_str_to_schema(schema_as_str):
    columns = schema_as_str.split(",")
    schema_fields = []
    for c in columns:
        name, type = c.strip().split(" ")
        datatype = map_string_type_to_datatype(type)
        schema_fields.append(StructField(name,datatype))
    return StructType(schema_fields)

from snowflake.snowpark import DataFrame
def display(data:DataFrame, limit:int=10):
    from IPython.display import display as _display, HTML
    if isinstance(data, DataFrame):
        try:
            output_cell_output = displayHTML
        except:
            output_cell_output = lambda x: _display(HTML(x))
        try:
            print(f"Output restricted to {limit} rows")
            html = data.limit(limit).to_pandas().to_html()
            output_cell_output(html)
        except Exception as e:
            print(f"Error displaying dataframe {e}")
    else:
        _display(data)