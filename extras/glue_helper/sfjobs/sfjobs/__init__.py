from snowflake.snowpark import Session, DataFrame as DynamicFrame
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name_without_upper_casing
from snowflake.snowpark.functions import try_cast, split, iff, typeof, col, object_construct, cast
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import snow_type_to_dtype_str
import logging
from snowflake.snowpark._internal.utils import quote_name
from sfjobs.transforms  import ApplyMapping, ResolveChoice
from snowflake.snowpark.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, BooleanType, DateType, TimestampType, VariantType, BinaryType

from snowflake.snowpark._internal.utils import SNOWFLAKE_PATH_PREFIXES
import re

## this is to extend the supported prefixes
if not "s3://" in SNOWFLAKE_PATH_PREFIXES:
    SNOWFLAKE_PATH_PREFIXES.append("s3://")

if not hasattr(DynamicFrame, '__sfjobs_extended__'):
    setattr(DynamicFrame, '__sfjobs_extended__', True)
    from snowflake.snowpark import DataFrame

    # Function to convert tick-quoted into double-quoted uppercase
    def convert_string(s):
        # Use regex to find the quoted string and convert it to uppercase
        return re.sub(r"`(.*?)`", lambda match: f'"{match.group(1).upper()}"', s)

    __sql = Session.sql
    def adjusted_sql(self, sql_text, *params):
        sql_text = convert_string(sql_text)
        return __sql(self, sql_text, *params)
    setattr(Session, 'sql', adjusted_sql)

    ___sql = DynamicFrame.filter
    def adjusted_filter(self, expr):
        sql_text = convert_string(expr)
        return ___sql(self, sql_text)
    setattr(DynamicFrame, 'filter', adjusted_filter)
    setattr(DynamicFrame, 'where', adjusted_filter)

    ## Adding case insensitive flag 
    def get_ci_property(self):
        return self._allow_case_insensitive_column_names
    def set_ci_property(self, value):
        self._allow_case_insensitive_column_names = value
    setattr(DynamicFrame,"get_ci_property",get_ci_property)
    setattr(DynamicFrame,"set_ci_property",set_ci_property)
    DynamicFrame.case_insensitive_resolution = property(get_ci_property, set_ci_property)

    ## Adding a method to get override default column resolution to enable also case insensitive search
    def _case_insensitive_resolve(self, col_name: str):
        normalized_col_name = quote_name(col_name)
        if hasattr(self, "_allow_case_insensitive_column_names") and self._allow_case_insensitive_column_names:
            normalized_col_name = normalized_col_name.upper()
            cols = list(filter(lambda attr: attr.name.upper() == normalized_col_name, self._output))
        else:
            cols = list(filter(lambda attr: attr.name == normalized_col_name, self._output))
        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                col_name
            )
    setattr(DynamicFrame,"_resolve",_case_insensitive_resolve)


    ## dummy method
    def fromDF(cls, dataframe, ctx, name):
        if name:
            logging.info(f"fromDF {name}")
        return dataframe
    DynamicFrame.fromDF = classmethod(fromDF)

    ## extends dataFrame class adding apply_mapping method
    def apply_mapping(self, mappings, case_insensitive=True):
        return ApplyMapping()(self, mappings, case_insensitive)
    setattr(DynamicFrame, "apply_mapping", apply_mapping)


    def resolveChoice(self: DataFrame, specs: list, ignore_case = True) -> DataFrame:
        return ResolveChoice()(self, specs, ignore_case)
        
    setattr(DynamicFrame,"resolveChoice",resolveChoice)
    
    ## patching toDF without arguments should just return the dataframe
    __df = DataFrame.to_df
    def updated_to_DF(self,*names):
        if len(names) == 0:
            return self
        else:
            return __df(self,*names)
    setattr(DynamicFrame,"to_df",updated_to_DF)
    setattr(DynamicFrame,"toDF",updated_to_DF)

    def rename_field(self, old_name, new_data, transformation_ctx="",info="",ignore_case=True,**kwargs):
        if len(kwargs):
            logging.warning(f"ignored kwargs: {kwargs}")
        if transformation_ctx:
            logging.info(f"CTX: {transformation_ctx}")
            self.session.append_query_tag(transformation_ctx,separator="|")
        if info:
            logging.info(info)
        logging.info(f"Renaming field {old_name} to {new_name}")
        if ignore_case:
            field = find_field(old_name,frame,ignore_case=ignore_case)
            return frame.withColumnRenamed(field.name, new_name)
        else:
            return frame.withColumnRenamed(old_name, new_name)
