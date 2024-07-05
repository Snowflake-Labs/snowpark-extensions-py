from .transform import SFTransform
import logging
from snowflake.snowpark import DataFrame

class DropNullFields(SFTransform):

    def __call__(self, frame:DataFrame, transformation_ctx:str = "", info:str = ""):
        if transformation_ctx:
            logging.info(f"CTX: {transformation_ctx}")
        return frame.dropna()
