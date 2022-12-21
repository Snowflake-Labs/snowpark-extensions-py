from snowflake.snowpark import DataFrame, Row, DataFrameNaFunctions, Column
from snowflake.snowpark.functions import col, lit, udtf, regexp_replace
from snowflake.snowpark import functions as F
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark.functions import table_function
from snowflake.snowpark.column import _to_col_if_str, _to_col_if_lit
import pandas as pd
import numpy as np
from snowpark_extensions.utils import map_to_python_type, schema_str_to_schema
import shortuuid
from snowflake.snowpark import context
from snowflake.snowpark.types import StructType,StructField
from snowflake.snowpark._internal.analyzer.expression import Expression
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

    
    class SpecialColumn(Column):
        def __init__(self,special_column_base_name="special_column"):
            self.special_col_name = _generate_prefix(special_column_base_name)
            super().__init__(self.special_col_name)
            self._is_special_column = True
            self._expression._is_special_column = True
            sc = self 
            def _expand(df):
                nonlocal sc
                return sc.expand(df)
            self._expression.expand = _expand
            self._special_column_dependencies=[]
            import uuid
            self._hash = hash(str(uuid.uuid4()))
        def __hash__(self):
            return self._hash
        def __eq__(self, other):
            return self._hash == other._hash
        def gen_unique_value_name(self,idx,base_name):
            return base_name if idx == 0 else f"{base_name}_{idx}"
        def add_columns(self,new_cols, alias:str=None):
           pass 
        def expand(self,df):
            pass
        @classmethod
        def extract_specials(cls,c):
            if isinstance(c, Expression):
                if c.children:
                    for child in c.children:
                        is_child_special = cls.has_special_column(child)
                        if is_child_special:
                           return True
            if hasattr(c,"_is_special_column"):
                return True
            if hasattr(c,"_expression") and c._expression and c._expression.children:
                  for child in c._expression.children:
                        is_child_special = cls.has_special_column(child)
                        if is_child_special:
                           return True
        @classmethod
        def any_specials(cls,*cols):
            for c in cols:
                for special in cls.specials(c):
                    return True
        @classmethod
        def specials(cls,c):
            if isinstance(c, Expression):
                if c.children:
                    for child in c.children:
                        for special in cls.specials(child):
                            yield special
            elif hasattr(c,"_expression") and c._expression.children:
                for child in c._expression.children:
                        for special in cls.specials(child):
                            yield special
            if hasattr(c,"_special_column_dependencies") and c._special_column_dependencies:
                     for dependency in c._special_column_dependencies:
                        for special in cls.specials(dependency):
                            yield special
            if hasattr(c,"_is_special_column"):
                yield c

    
    class ArraySort(SpecialColumn):
        def __init__(self,array_col):
            super().__init__("sorted")
            self.array_col = array_col
            self._special_column_dependencies = [array_col]
        def add_columns(self, new_cols, alias:str = None):
            # add itself as column
            new_cols.append(self.alias(alias) if alias else self)
        def expand(self,df):
            array_col = _to_col_if_str(self.array_col, "array_sort")
            df = df.with_column("__IDX",F.seq8())
            flatten = table_function("flatten")
            df_array_sorted=df.join_table_function(flatten(input=array_col,outer=lit(True))).group_by("__IDX").agg(F.sql_expr("array_agg(value) within group(order by value)").alias(self.special_col_name))
            df = df.join(df_array_sorted,on="__IDX").drop("__IDX")
            return df
     
    class ArrayFlatten(SpecialColumn):
        def __init__(self,flatten_col,remove_arrays_when_there_is_a_null):
            super().__init__("flatten")
            self.flatten_col = flatten_col
            self.remove_arrays_when_there_is_a_null = remove_arrays_when_there_is_a_null
            self._special_column_dependencies = [flatten_col]
        def add_columns(self, new_cols, alias:str = None):
            new_cols.append(self.alias(alias) if alias else self)
        def expand(self,df):
            array_col = _to_col_if_str(self.flatten_col, "flatten")
            flatten = table_function("flatten")
            df=df.join_table_function(flatten(array_col).alias("__SEQ_FLATTEN","KEY","PATH","__INDEX_FLATTEN","__FLATTEN_VALUE","THIS"))
            df = df.drop("KEY","PATH","THIS")
            if self.remove_arrays_when_there_is_a_null:
                df_with_has_null=df.withColumn("__HAS_NULL",has_null(array_col))
                df_flattened= df_with_has_null.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("BOOLOR_AGG",col("__HAS_NULL")).alias("__HAS_NULL"),F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                df_flattened=df_flattened.with_column("__FLATTEN_VALUE",F.iff("__HAS_NULL", lit(None), col("__FLATTEN_VALUE"))).drop("__HAS_NULL")
                df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.special_col_name)
                return df
            else:
                df_flattened= df.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.special_col_name)
                return df

    class ArrayZip(SpecialColumn):
        def __init__(self,left,*right,use_compat=False):
            super().__init__("zipped")
            self.left_col  = left
            self.right_cols = right
            self._special_column_dependencies = [left,*right]
            self._use_compat = use_compat
        def add_columns(self,new_cols,alias:str = None):
            new_cols.append(self.alias(alias) if alias else self)
        def expand(self,df):
            if (not hasattr(ArrayZip,"_added_compat_func")) and self._use_compat:
                context.get_active_session().sql("""
CREATE OR REPLACE TEMPORARY FUNCTION PUBLIC.ARRAY_UNDEFINED_COMPACT(ARR VARIANT) RETURNS ARRAY
LANGUAGE JAVASCRIPT AS
$$
    if (ARR.includes(undefined)){
        filtered = ARR.filter(x => x === undefined);
        if (filtered.length==0)
            return filtered;
    }
    return ARR;
$$;                
                """).count()
                ArrayZip._added_compat_func = True
            df_with_idx = df.with_column("_IDX",F.seq8())
            flatten = table_function("flatten")
            right = df_with_idx.select("_IDX",self.left_col)\
                .join_table_function(flatten(input=self.left_col,outer=lit(True))\
                .alias("SEQ","KEY","PATH","INDEX","__VALUE_0","THIS")) \
                .drop(self.left_col,"SEQ","KEY","PATH","THIS")
            vals=["__VALUE_0"]
            for right_col in self.right_cols:
                prior=len(vals)-1
                next=len(vals)
                left_col_name=f"__VALUE_{prior}"
                right_col_name=f"__VALUE_{next}"
                vals.append(right_col_name)
                new_right=df_with_idx.select("_IDX",right_col).join_table_function(flatten(input=right_col,outer=lit(False))\
                    .alias("SEQ","KEY","PATH","INDEX",right_col_name,"THIS")) \
                    .drop(right_col,"SEQ","KEY","PATH","THIS") #.with_column("INDEX",F.coalesce(col("INDEX"),lit(0))) \
                if right:
                    right = right.join(new_right,on=["_IDX","INDEX"],how="left",lsuffix="___LEFT")
                else:
                    right = new_right
            zipped = right.select("_IDX","INDEX",F.array_construct(*vals).alias("NGROUP"))
            if self._use_compat:
              zipped = zipped.with_column("NGROUP",F.call_builtin("ARRAY_UNDEFINED_COMPACT",col("NGROUP")))  
            zipped=zipped.group_by("_IDX").agg(F.sql_expr(f'arrayagg(ngroup) within group (order by INDEX) {self.special_col_name}'))
            result = df_with_idx.join(zipped,on="_IDX").drop("_IDX")
            return result
        
    class Explode(SpecialColumn):
        def __init__(self,expr,map=False,outer=False,use_compat=False):
            """ Right not it must be explictly stated if the value is a map. By default it is assumed it is not"""
            super().__init__("value" if map else "col")
            self.expr = expr
            self.map = map
            self.outer = outer
            self.key_col_name = None
            self.key_col = None
            self.value_col_name = self.special_col_name
            self._special_column_dependencies = [expr]
            self.use_compat=use_compat
        def add_columns(self,new_cols,alias:str = None):
            if self.map:
                self.key_col_name = _generate_prefix("key")
                self.key_col = col(self.key_col_name)
                new_cols.append(self.key_col.alias(alias + "_1") if alias else self.key_col)
            new_cols.append(self.alias(alias) if alias else self)
        def expand(self,df):
            if self.key_col_name is not None:
                df = df.join_table_function(flatten(input=self.expr,outer=lit(self.outer)).alias("SEQ",self.key_col_name,"PATH","INDEX",self.value_col_name,"THIS")).drop(["SEQ","PATH","INDEX","THIS"])
            else:
                df = df.join_table_function(flatten(input=self.expr,outer=lit(self.outer)).alias("SEQ","KEY","PATH","INDEX",self.value_col_name,"THIS")).drop(["SEQ","KEY","PATH","INDEX","THIS"])
                if self.use_compat:
                    df=df.with_column(self.value_col_name,
                       F.iff(
                        F.cast(self.value_col_name,ArrayType()) == F.array_construct(),
                        lit(None),
                        F.cast(self.value_col_name,ArrayType())))
            return df

    def explode(expr,outer=False,map=False,use_compat=False):
        return Explode(expr,map,outer,use_compat=use_compat)

    def explode_outer(expr,map=False, use_compat=False):
        return Explode(expr,map,True,use_compat=use_compat)

    F.explode = explode
    F.explode_outer = explode_outer
    def _arrays_zip(left,*right,use_compat=False):
        """ In SF zip might return [undefined,...undefined] instead of [] """
        return ArrayZip(left,*right,use_compat=use_compat)
    def _arrays_flatten(array_col,remove_arrays_when_there_is_a_null=True):
        return ArrayFlatten(array_col,remove_arrays_when_there_is_a_null)
    def _array_sort(array_col):
        return ArraySort(array_col)

    F.arrays_zip = _arrays_zip
    F.flatten    = _arrays_flatten
    F.array_sort = _array_sort
    flatten = table_function("flatten")
    _oldwithColumn = DataFrame.withColumn
    _oldSelect = DataFrame.select
    def withColumnExtended(self,colname,expr):
        if isinstance(expr, SpecialColumn):
            new_cols = []
            expr.add_columns(new_cols, alias=colname)
            df = self
            for s in SpecialColumn.specials(expr):
                df=s.expand(df)
            return _oldSelect(df,*self.columns,*new_cols)
            #return self.with_columns(df,new_cols,[col(x) for x in new_cols])
        else:
            return _oldwithColumn(self,colname,expr)
        
    DataFrame.withColumn = withColumnExtended

    

    def selectExtended(self,*cols):
        ## EXPLODE
        if SpecialColumn.any_specials(*cols):
          new_cols = []
          extended_cols = []
          # extend only the main cols
          for x in cols:
            if isinstance(x, SpecialColumn):
                x.add_columns(new_cols)
                extended_cols.append(x)
            else:
                new_cols.append(x)
          df = self
          # but expand all tables, because there could be several joins
          already = set()
          for c in cols:
            for extended_col in SpecialColumn.specials(c):
                if not extended_col in already:
                    already.add(extended_col)
                    df = extended_col.expand(df)
          return _oldSelect(df,*[_to_col_if_str(x,"extended") for x in new_cols])
        else:
            return _oldSelect(self,*cols)
    
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
