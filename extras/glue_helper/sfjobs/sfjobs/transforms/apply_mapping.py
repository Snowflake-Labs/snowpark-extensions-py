
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import sql_expr, lit, col, object_construct
from .transform import SFTransform

import logging
from snowflake.snowpark._internal.utils import quote_name


class ApplyMapping(SFTransform):
    def map_type(self, type_name:str):
        if type_name == "long":
            return "int"
        return type_name
    def record_nested_mapping(self, source_field:str, source_type:str, target_field:list, target_type:str,ctx:dict):
        if len(target_field) == 1:
            target_field = target_field[0]
            ctx[target_field] = (source_field, source_type, target_field, target_type)
        else:
            current_field = target_field.pop(0)
            if not current_field in ctx:
                ctx[current_field] = {}
            self.record_nested_mapping(source_field, source_type, target_field, target_type, ctx[current_field])
    def to_object_construct(self,mapping,case_insensitive=True):
        if isinstance(mapping, dict):
           new_data = []
           for key in mapping:
               data = mapping[key]
               if isinstance(data, dict):
                   new_data.append(lit(key))
                   new_data.append(self.to_object_contruct(key, data))
               elif isinstance(data, tuple):
                   source_field, source_type, target_field, target_type = data
                   if case_insensitive:
                       target_field = target_field.upper()
                   new_data.append(lit(target_field))
                   if case_insensitive:
                       source_field = quote_name(source_field.upper())
                   target_type = self.map_type(target_type)
                   new_data.append(sql_expr(f'{source_field}::{target_type}'))
           return object_construct(*new_data)
    def __call__(cls, frame:DataFrame, mappings, transformation_ctx:str="", case_insensitive=True):
        if transformation_ctx:
            logging.info(f"CTX: {transformation_ctx}")
        column_mappings = []
        column_names = []

        nested_mappings = {}
        final_columns = []
        for source_field, source_type, target_field, target_type in mappings:
            if case_insensitive:
                target_field = target_field.upper()
            if '.' in target_field:
                # nesting
                target_parts = target_field.split('.')
                cls.record_nested_mapping(source_field, source_type, target_field.split('.'), target_type, nested_mappings)
                if target_parts[0] not in final_columns:
                    final_columns.append(target_parts[0])
            else:
                if case_insensitive:
                    target_field = target_field.upper()
                column_names.append(target_field)
                if case_insensitive:
                    source_field = quote_name(source_field.upper())
                target_type = cls.map_type(target_type)
                column_mappings.append(sql_expr(f'{source_field}::{target_type}'))
                final_columns.append(target_field)
        for new_struct_key in nested_mappings:
            column_names.append(new_struct_key)
            column_mappings.append(cls.to_object_construct(nested_mappings[new_struct_key],case_insensitive))
            
        return frame.with_columns(column_names, column_mappings).select(final_columns)