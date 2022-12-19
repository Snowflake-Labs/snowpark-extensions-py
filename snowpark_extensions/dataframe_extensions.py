from snowflake.snowpark import DataFrame, Row, DataFrameNaFunctions
from snowflake.snowpark.functions import col, lit, udtf, regexp_replace
from snowflake.snowpark import functions as F
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark.functions import table_function
from snowflake.snowpark.column import _to_col_if_str, _to_col_if_lit
import pandas as pd
import numpy as np
from snowpark_extensions.utils import map_to_python_type, schema_str_to_schema
import shortuuid
from snowflake.snowpark.types import StructType,StructField
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    overload,
)
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
)

if not hasattr(DataFrame,"___extended"):
    DataFrame.___extended = True
    def get_dtypes(schema):
        data = np.array([map_to_python_type(x.datatype) for x in schema.fields])
        # providing an index
        dtypes_series = pd.Series(data, index=schema.names)
        return dtypes_series

    DataFrame.dtypes = property(lambda self: get_dtypes(self.schema))


    def map(self,func,output_types,input_types=None,input_cols=None,to_row=False):
        clazz="map"+shortuuid.uuid()[:8]
        output_schema=[]
        if not input_types:
            input_types = [x.datatype for x in self.schema.fields]
        input_cols_len=len(input_types)
        if not input_cols:
            input_col_names=self.columns[:input_cols_len] 
            _input_cols = [self[x] for x in input_col_names]
        else:
            input_col_names=input_cols
            _input_cols = input_cols
        output_schema = [StructField(f"c_{i+1}",output_types[i]) for i in range(len(output_types))]
        output_cols = [col(x.name) for x in output_schema]
        if to_row:
            myRow = Row(*input_col_names)
            def process_func(self,*argv):
                yield func(myRow(*argv))
        else:
            def process_func(self,*argv):
                yield func(*argv)
        udtf_class = type(clazz, (object, ), {"process":process_func})
        udtf(udtf_class,output_schema=StructType(output_schema), input_types=input_types,name=clazz,replace=True,packages=["snowflake-snowpark-python"])
        return self.join_table_function(clazz,*_input_cols).select(*output_cols)

    DataFrame.map = map

    def simple_map(self,func):
        return self.select(*func(self))

    DataFrame.simple_map = simple_map

    DataFrameNaFunctions.__oldreplace = DataFrameNaFunctions.replace 
    
    def extended_replace(
        self,
        to_replace: Union[
            LiteralType,
            Iterable[LiteralType],
            Dict[LiteralType, LiteralType],
        ],
        value: Optional[Iterable[LiteralType]] = None,
        subset: Optional[Iterable[str]] = None,
        regex=False
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        if regex:
            return self._df.select([regexp_replace(col(x.name), to_replace,value).alias(x.name) if isinstance(x.datatype,StringType) else col(x.name) for x in self._df.schema])
        else:
            return self.__oldreplace(to_replace,value,subset)
    
    DataFrameNaFunctions.replace = extended_replace

    def has_null(col):
        return F.array_contains(F.sql_expr("parse_json('null')"),col) | F.coalesce(F.array_contains(lit(None) ,col),lit(False))

    # SPECIAL COLUMN HELPERS
    class SpecialColumn():
        def _is_special_column():
            return True
        def gen_unique_value_name(self,idx,base_name):
            return base_name if idx == 0 else f"{base_name}_{idx}"
        def add_columns(self,new_cols, idx):
           pass 
        def expand(self,df):
            pass
    
    class ArraySort(SpecialColumn):
        def __init__(self,array_col):
            self.array_col = array_col
        def add_columns(self, new_cols, idx):
            self.value_col_name = self.gen_unique_value_name(idx,"sorted")
            new_cols.append(self.value_col_name)
        def expand(self,df):
            array_col = _to_col_if_str(self.array_col, "array_sort")
            df = df.with_column("__IDX",F.seq8())
            flatten = table_function("flatten")
            df_array_sorted=df.join_table_function(flatten(input=array_col,outer=lit(True))).group_by("__IDX").agg(F.sql_expr("array_agg(value) within group(order by value)").alias("sorted"))
            df = df.join(df_array_sorted,on="__IDX").drop("__IDX")
            return df
     
    class ArrayFlatten(SpecialColumn):
        def __init__(self,flatten_col,remove_arrays_when_there_is_a_null):
            self.flatten_col = flatten_col
            self.remove_arrays_when_there_is_a_null = remove_arrays_when_there_is_a_null
        def add_columns(self, new_cols, idx):
            self.value_col_name = self.gen_unique_value_name(idx,"flatten")
            new_cols.append(self.value_col_name)
        def expand(self,df):
            array_col = _to_col_if_str(self.flatten_col, "flatten")
            flatten = table_function("flatten")
            df=df.join_table_function(flatten(array_col).alias("__SEQ_FLATTEN","KEY","PATH","__INDEX_FLATTEN","__FLATTEN_VALUE","THIS"))
            df = df.drop("KEY","PATH","THIS")
            if self.remove_arrays_when_there_is_a_null:
                df_with_has_null=df.withColumn("__HAS_NULL",has_null(array_col))
                df_flattened= df_with_has_null.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("BOOLOR_AGG",col("__HAS_NULL")).alias("__HAS_NULL"),F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                df_flattened=df_flattened.with_column("__FLATTEN_VALUE",F.iff("__HAS_NULL", lit(None), col("__FLATTEN_VALUE"))).drop("__HAS_NULL")
                df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.value_col_name)
                return df
            else:
                df_flattened= df.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.value_col_name)
                return df

    class ArrayZip(SpecialColumn):
        def __init__(self,left,*right):
            self.left_col  = left
            self.right_cols = right
            #self.right_col = right
        def add_columns(self,new_cols,idx):
            self.value_col_name = self.gen_unique_value_name(idx,"zipped")
            new_cols.append(self.value_col_name)
        def expand(self,df):
            flatten = table_function("flatten")
            left = df.join_table_function(flatten(self.left_col)\
            .alias("SEQ","KEY","PATH","INDEX","__VALUE_0","THIS")) \
            .orderBy("SEQ","INDEX") \
            .with_column("__IDX",F.seq8()) \
            .drop("SEQ","KEY","PATH","INDEX","THIS")
            vals=["__VALUE_0"]
            for right_col in self.right_cols:
                prior=len(vals)-1
                next=len(vals)
                left_col_name=f"__VALUE_{prior}"
                right_col_name=f"__VALUE_{next}"
                vals.append(right_col_name)
                right=df.select(right_col).join_table_function(flatten(right_col)\
                    .alias("SEQ","KEY","PATH","INDEX",right_col_name,"THIS")) \
                    .orderBy("SEQ","INDEX") \
                    .select(F.seq8().alias("__IDX"),col(right_col_name))
                left=left.join(right,on="__IDX",how="left",lsuffix="___LEFT")
            zipped = left.with_column("ZIPPED",F.array_construct(*vals))\
                    .drop(*vals,"__IDX")
            return zipped
        
    class Explode(SpecialColumn):
        def __init__(self,expr,map=False,outer=False):
            """ Right not it must be explictly stated if the value is a map. By default it is assumed it is not"""
            self.expr = expr
            self.map = map
            self.outer = outer
        def add_columns(self,new_cols,idx):
            self.value_col_name = self.gen_unique_value_name(idx,"value" if self.map else "col")
            self.key_col_name = None
            if self.map:
                self.key_col_name = self.gen_unique_value_name(idx,"key")
                new_cols.append(self.key_col_name)
            new_cols.append(self.value_col_name)
        def expand(self,df):
            if self.key_col_name:
                df = df.join_table_function(flatten(input=self.expr,outer=lit(self.outer)).alias("SEQ",self.key_col_name,"PATH","INDEX",self.value_col_name,"THIS")).drop(["SEQ","PATH","INDEX","THIS"])
            else:
                df = df.join_table_function(flatten(input=self.expr,outer=lit(self.outer)).alias("SEQ","KEY","PATH","INDEX",self.value_col_name,"THIS")).drop(["SEQ","KEY","PATH","INDEX","THIS"])
            return df

    def explode(expr,outer=False,map=False):
        return Explode(expr,map,outer)

    def explode_outer(expr,map=False):
        return Explode(expr,map,True)

    F.explode = explode
    F.explode_outer = explode_outer
    def _arrays_zip(left,*right):
        return ArrayZip(left,*right)
    def _arrays_flatten(array_col,remove_arrays_when_there_is_a_null=True):
        return ArrayFlatten(array_col,remove_arrays_when_there_is_a_null)
    def _array_sort(array_col):
        return ArraySort(array_col)

    F.arrays_zip = _arrays_zip
    F.flatten    = _arrays_flatten
    F.array_sort = _array_sort
    flatten = table_function("flatten")
    _oldwithColumn = DataFrame.withColumn
    def withColumnExtended(self,colname,expr):
        if isinstance(expr, SpecialColumn):
            new_cols = []
            expr.add_columns(new_cols, 0)
            df=expr.expand(self)
            return self.withColumns(df,new_cols,[col(x) for x in new_cols])
        else:
            return _oldwithColumn(self,colname,expr)
        
    DataFrame.withColumn = withColumnExtended

    oldSelect = DataFrame.select

    def selectExtended(self,*cols):
        ## EXPLODE
        if any(isinstance(x, SpecialColumn) for x in cols):
          new_cols = []
          extended_cols = []
          for x in cols:
            if isinstance(x, SpecialColumn):
                x.add_columns(new_cols,len(extended_cols))
                extended_cols.append(x)
            else:
                new_cols.append(x)
          df = self
          for extended_col in extended_cols:
            df = extended_col.expand(df)
          return oldSelect(df,*[_to_col_if_str(x,"extended") for x in new_cols])
        else:
            return oldSelect(self,*cols)
    
    DataFrame.select = selectExtended


import shortuuid
from snowflake.snowpark import Window, Column
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udtf, col
from snowflake.snowpark.relational_grouped_dataframe import RelationalGroupedDataFrame

if not hasattr(RelationalGroupedDataFrame, "applyInPandas"):
  def applyInPandas(self,func,schema):
      output_schema = schema
      if isinstance(output_schema, str):
        output_schema = schema_str_to_schema(output_schema)
      from snowflake.snowpark.functions import col
      input_types = [x.datatype for x in self._df.schema.fields]
      input_cols  = [x.name for x in self._df.schema.fields]
      output_cols = [x.name for x in output_schema.fields]
      grouping_exprs = [Column(x) for x in self._grouping_exprs]
      clazz="applyInPandas"+shortuuid.uuid()[:8]
      def __init__(self):
          self.rows = []
          self.dfs  = []
      def process(self, *row):
          self.rows.append(row)
          # Merge rows into a dataframe
          if len(self.rows) >= 16000:
             df = pd.DataFrame(self.rows)
             self.dfs.append(df)
             self.rows = []
          # Merge dataframes into a single dataframe
          if len(self.dfs) >= 100:
             merged_df = pd.concat(self.dfs)
             self.dfs = [merged_df]
          yield None
      def end_partition(self):
        # Merge any remaining rows
        if len(self.rows) > 0:
          df = pd.DataFrame(self.rows,columns=input_cols)
          self.dfs.append(df)
          self.rows = []
        pandas_input = pd.concat(self.dfs)        
        pandas_output = func(pandas_input)
        for row in pandas_output.itertuples(index=False):
             yield tuple(row)
      non_ambigous_output_schema = StructType([StructField(f"pd_{i}",output_schema.fields[i].datatype) for i in range(len(output_schema.fields))])
      renamed_back = [col(f"pd_{i}").alias(output_schema.fields[i].name) for i in range(len(non_ambigous_output_schema.fields))]
      udtf_class = type(clazz, (object, ), {"__init__":__init__,"process":process,"end_partition":end_partition})
      tfunc = udtf(udtf_class,output_schema=non_ambigous_output_schema, input_types=input_types,name=clazz,replace=True,is_permanent=False,packages=["snowflake-snowpark-python", "pandas"])
      return self._df.join_table_function(tfunc(*input_cols).over(partition_by=grouping_exprs, order_by=grouping_exprs)).select(*renamed_back)

  RelationalGroupedDataFrame.applyInPandas = applyInPandas
  ###### HELPER END   
