class SFTransform(object):
    """Base class for all Transforms.

    All transformations should inherit from SFTransform and define a
    __call__ method. They can optionally override the name classmethod or use
    the default of the class name.
    """

    @classmethod
    def apply(cls, *args, **kwargs):
        transform = cls()
        return transform(*args, **kwargs)

    @classmethod
    def name(cls):
        return cls.__name__

from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name_without_upper_casing
from snowflake.snowpark.types import StringType
def find_field(col_name,df,ignore_case=False):
    current_field = None
    for f in df.schema.fields:
        if ignore_case and f.name.upper() == quote_name_without_upper_casing(col_name).upper():
            current_field = f
            break
        elif f.name == col_name:
            current_field = f
            break
    if not current_field:
        raise Exception(f"Field {col_name} not found")
    if isinstance(current_field.datatype, StringType):
        current_field.datatype.length = None
    return current_field