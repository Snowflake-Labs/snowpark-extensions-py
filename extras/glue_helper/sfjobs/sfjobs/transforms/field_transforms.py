from snowflake.snowpark import Session, DataFrame
import logging 
from .transform import SFTransform, find_field
from functools import reduce
from snowflake.snowpark.functions import col

class SelectFields(SFTransform):
    """
    Get fields within a DataFrame

    :param frame: DataFrame
    :param paths: List of Strings or Columns
    :param info: String, any string to be associated with errors in this transformation.
    :return: DataFrame
    """
    def __call__(self, frame, paths, transformation_ctx = "", info = ""):
        if transformation_ctx:
            logging.info(f"CTX: {transformation_ctx}")
            frame.session.append_query_tag(transformation_ctx,separator="|")
        if info:
            logging.info(info)
        logging.info(f"Selecting fields {paths}")
        return frame.select(*paths)

class RenameField(SFTransform):
    """
    Rename fields within a DataFrame
    :return: DataFrame
    """
    def __call__(self, frame, old_name, new_name, transformation_ctx = "", info = "",ignore_case=True, **kwargs):
        return frame.rename_field(old_name, new_name, transformation_ctx, info, ignore_case,**kwargs)

class Join(SFTransform):
    def __call__(self, frame1, frame2,  keys1, keys2, transformation_ctx = ""):
        assert len(keys1) == len(keys2), "The keys lists must be of the same length"
        comparison_expression = reduce(lambda expr, ids: expr & (col(ids[0]) == col(ids[1])), zip(list1, list2), col(list1[0]) == col(list2[0]))
        return frame1.join(frame2, on=comparison_expression)