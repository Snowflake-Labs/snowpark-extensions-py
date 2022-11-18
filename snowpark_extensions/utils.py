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
    TimestampType,
    TimeType,
    VariantType,
)
import datetime
import decimal
# based on https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing.html#label-sql-python-data-type-mappings

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
