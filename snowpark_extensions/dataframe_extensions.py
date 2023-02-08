from snowflake.snowpark import DataFrame, Row, DataFrameNaFunctions, Column
from snowflake.snowpark.functions import col, lit, udtf, regexp_replace
from snowflake.snowpark import functions as F
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark.functions import table_function, udf
from snowflake.snowpark.column import _to_col_if_str, _to_col_if_lit
import pandas as pd
import numpy as np
from snowpark_extensions.utils import map_to_python_type, schema_str_to_schema
from snowflake.snowpark import context
from snowflake.snowpark.types import StructType,StructField
from snowflake.snowpark._internal.analyzer.expression import Expression, FunctionExpression
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name

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

from snowflake.snowpark._internal.utils import (
   parse_positional_args_to_list
)

from snowflake.snowpark.table_function import (
    TableFunctionCall,
    _create_table_function_expression,
    _get_cols_after_join_table,
)

from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionJoin
)

from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectStatement,
    SelectSnowflakePlan
)

if not hasattr(DataFrame,"___extended"):
    
    DataFrame.___extended = True

    # we need to extend the alias function for
    # table function to allow the situation where
    # the function returns several columns
    def adjusted_table_alias(self,*aliases) -> "TableFunctionCall":
        canon_aliases = [quote_name(col) for col in aliases]
        if len(set(canon_aliases)) != len(aliases):
            raise ValueError("All output column names after aliasing must be unique.")
        if hasattr(self, "alias_adjust"):
            """
            currently tablefunctions are rendered as table(func(....))
            One option later on could be to render this is (select col1,col2,col3,col4 from table(func(...)))
            aliases can the be use as (select col1 alias1,col2 alias2 from table(func(...)))
            """
            self._aliases = self.alias_adjust(*canon_aliases)
        else:
            self._aliases = canon_aliases
        return self

    TableFunctionCall.alias = adjusted_table_alias
    TableFunctionCall.as_   = adjusted_table_alias

    def get_dtypes(schema):
        data = np.array([map_to_python_type(x.datatype) for x in schema.fields])
        # providing an index
        dtypes_series = pd.Series(data, index=schema.names)
        return dtypes_series

    DataFrame.dtypes = property(lambda self: get_dtypes(self.schema))


    def map(self,func,output_types,input_types=None,input_cols=None,to_row=False):
        clazz= _generate_prefix("map")
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
            self.expanded = False
        def __hash__(self):
            return self._hash
        def __eq__(self, other):
            return self._hash == other._hash
        def gen_unique_value_name(self,idx,base_name):
            return base_name if idx == 0 else f"{base_name}_{idx}"
        def add_columns(self,new_cols, alias:str=None):
           pass
        def expand(self,df):
            self.expanded = True
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
            """  Returns special columns that might add a join clause to the dataframe """
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

    class MapValues(SpecialColumn):
        def __init__(self,array_col):
            super().__init__("values")
            self.array_col = array_col
            self._special_column_dependencies = [array_col]
        def add_columns(self, new_cols, alias:str = None):
            # add itself as column
            new_cols.append(self.alias(alias) if alias else self)
        def expand(self,df):
            if not self.expanded:
                array_col = _to_col_if_str(self.array_col, "values")
                df = df.with_column("__IDX",F.seq8())
                flatten = table_function("flatten")
                seq=_generate_prefix("SEQ")
                key=_generate_prefix("KEY")
                path=_generate_prefix("PATH")
                index=_generate_prefix("INDEX")
                value=_generate_prefix("VALUE")
                this=_generate_prefix("THIS")
                df_values=df.join_table_function(flatten(input=array_col,outer=lit(True)).alias(seq,key,path,index,value,this)).group_by("__IDX").agg(F.array_agg(value).alias(self.special_col_name)).distinct()
                df = df.join(df_values,on="__IDX").drop("__IDX")
                self.expanded = True
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
            if not self.expanded:
                array_col = _to_col_if_str(self.flatten_col, "flatten")
                flatten = table_function("flatten")
                df=df.join_table_function(flatten(array_col).alias("__SEQ_FLATTEN","KEY","PATH","__INDEX_FLATTEN","__FLATTEN_VALUE","THIS"))
                df = df.drop("KEY","PATH","THIS")
                if self.remove_arrays_when_there_is_a_null:
                    df_with_has_null=df.withColumn("__HAS_NULL",has_null(array_col))
                    df_flattened= df_with_has_null.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("BOOLOR_AGG",col("__HAS_NULL")).alias("__HAS_NULL"),F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                    df_flattened=df_flattened.with_column("__FLATTEN_VALUE",F.iff("__HAS_NULL", lit(None), col("__FLATTEN_VALUE"))).drop("__HAS_NULL")
                    df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.special_col_name)
                else:
                    df_flattened= df.group_by(col("__SEQ_FLATTEN")).agg(F.call_builtin("ARRAY_UNION_AGG",col("__FLATTEN_VALUE")).alias("__FLATTEN_VALUE"))
                    df=df.drop("__FLATTEN_VALUE").where(col("__INDEX_FLATTEN")==0).join(df_flattened,on="__SEQ_FLATTEN").drop("__SEQ_FLATTEN","__INDEX_FLATTEN").rename("__FLATTEN_VALUE",self.special_col_name)
                self.expanded = True
            return df


    def explode(expr,outer=False,map=False,use_compat=False):
        value_col = "explode"
        if map:
            key = "key"
            value_col = "value"
        else:
            key = _generate_prefix("KEY")
        seq = _generate_prefix("SEQ")
        path = _generate_prefix("PATH")
        index = _generate_prefix("INDEX")
        this = _generate_prefix("THIS")
        flatten = table_function("flatten")
        explode_res = flatten(input=expr,outer=lit(outer)).alias(seq,key,path,index,value_col,this)
        # we patch the alias, to simplify explode use case where only one column is used
        if not map:
            explode_res.alias_adjust = lambda alias1 : [seq,key,path,index,alias1,this] 
        # post action to execute after join
        def post_action(df):
            drop_columns = [seq,path,index,this] if map else [seq,key,path,index,this]
            df = df.drop(drop_columns)
            if use_compat:
                # in case we need backwards compatibility with spark behavior
                df=df.with_column(value_col,
                F.iff(F.cast(value_col,ArrayType()) == F.array_construct(),lit(None),F.cast(value_col,ArrayType())))
            return df
        explode_res.post_action = post_action
        return explode_res

    def explode_outer(expr,map=False, use_compat=False):
        return explode(expr,outer=True,map=map,use_compat=use_compat)

    F.explode = explode
    F.explode_outer = explode_outer
    # def _arrays_zip(left,*right,use_compat=False):
    #     """ In SF zip might return [undefined,...undefined] instead of [] """
    #     return ArrayZip(left,*right,use_compat=use_compat)
    def _arrays_flatten(array_col,remove_arrays_when_there_is_a_null=True):
        return ArrayFlatten(array_col,remove_arrays_when_there_is_a_null)
    def _map_values(col:ColumnOrName):
        col = _to_col_if_str(col,"map_values")
        return MapValues(col)

    F.map_values = _map_values
    F.arrays_zip = arrays_zip
    F.flatten    = _arrays_flatten
    flatten = table_function("flatten")
    _oldwithColumn = DataFrame.withColumn
    _oldSelect = DataFrame.select
    
    def selectExtended(self,*cols) -> "DataFrame":
        exprs = parse_positional_args_to_list(*cols)
        if not exprs:
            raise ValueError("The input of select() cannot be empty")
        names = []
        table_func = None
        join_plan = None
        for e in exprs:
            if isinstance(e, Column):
                names.append(e._named())
            elif isinstance(e, str):
                names.append(Column(e)._named())
            elif isinstance(e, TableFunctionCall):
                if table_func:
                    raise ValueError(
                        f"At most one table function can be called inside a select(). "
                        f"Called '{table_func.name}' and '{e.name}'."
                    )
                table_func = e
                func_expr = _create_table_function_expression(func=table_func)
                join_plan = self._session._analyzer.resolve(
                    TableFunctionJoin(self._plan, func_expr)
                )
                _, new_cols = _get_cols_after_join_table(
                    func_expr, self._plan, join_plan
                )
                names.extend(new_cols)
            else:
                raise TypeError(
                    "The input of select() must be Column, column name, TableFunctionCall, or a list of them"
                )
        if self._select_statement:
            if join_plan:
                result=self._with_plan(
                    SelectStatement(
                        from_=SelectSnowflakePlan(
                            join_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ).select(names)
                )
                if table_func and hasattr(table_func,"post_action"):
                    result = table_func.post_action(result)
                return result
            return self._with_plan(self._select_statement.select(names))

        result = self._with_plan(Project(names, join_plan or self._plan))
        if table_func and hasattr(table_func,"post_action"):
           result = table_func.post_action(result)
        return result





    DataFrame.select = selectExtended


from snowflake.snowpark import Window, Column
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udtf, col
from snowflake.snowpark.relational_grouped_dataframe import RelationalGroupedDataFrame

def group_by_pivot(self,pivot_col):
      return GroupByPivot(self, pivot_col)
RelationalGroupedDataFrame.pivot = group_by_pivot

class GroupByPivot():
      def __init__(self,old_groupby_col,pivot_col):
            self.old_groupby_col = old_groupby_col
            self.pivot_col=pivot_col
            group_by_exprs = [F.sql_expr(x.sql) for x in old_groupby_col._grouping_exprs]
            group_by_exprs.append(pivot_col)
            self.df = old_groupby_col._df.groupBy(group_by_exprs)
      def clean(self,pivoted):
        def get_valid_id(id):
            return id if re.match("[A-Za-z]\w+", id) else f'"{id}"'
        n = len(self.value_list)
        last_n_cols = self.value_list[-n:]
        pivoted_cols = pivoted.columns[-n:]
        previous_cols = pivoted.columns[:len(pivoted.columns)-n]
        for i in range(0,n):
            the_col = pivoted_cols[i]
            the_value = self.value_list[i]
            pivoted_cols[i] = col(the_col).alias(get_valid_id(str(the_value)))
        return pivoted.select(*previous_cols,*pivoted_cols)
      def prepare(self,col):
            first = self.df.agg(col.alias("__firstAggregate"))
            self.value_list = [x[0] for x in first.select(self.pivot_col).distinct().sort(self.pivot_col).collect()]
            return first.pivot(pivot_col=self.pivot_col,values=self.value_list)
      def avg(self, col: ColumnOrName) -> DataFrame:
            """Return the average for the specified numeric columns."""
            return self.prepare(F.avg(col)).avg("__firstAggregate")
      mean = avg
      def sum(self, col: ColumnOrName) -> DataFrame:
            """Return the sum for the specified numeric columns."""
            return  self.clean(self.prepare(F.sum(col)).sum("__firstAggregate"))
      def median(self, col: ColumnOrName) -> DataFrame:
            """Return the median for the specified numeric columns."""
            return self.clean(self.prepare(F.median(col)).median("__firstAggregate"))
      def min(self, col: ColumnOrName) -> DataFrame:
            """Return the min for the specified numeric columns."""
            return self.clean(self.prepare(F.min(col)).min("__firstAggregate"))
      def max(self, col: ColumnOrName) -> DataFrame:
            """Return the max for the specified numeric columns."""
            return self.clean(self.prepare(col).max("__firstAggregate"))
      def count(self) -> DataFrame:
            """Return the number of rows for each group."""
            return self.clean(self.prepare(col).count("__firstAggregate"))
      def agg(self, aggregated_col: ColumnOrName) -> DataFrame:
            if hasattr(aggregated_col, "_expression") and isinstance(aggregated_col._expression, FunctionExpression):
                name = aggregated_col._expression.name
                return self.clean(self.prepare(aggregated_col).function(name)(col("__firstAggregate")))
            else:
                raise Exception("Also functions expressions are supported")

if not hasattr(RelationalGroupedDataFrame, "applyInPandas"):
  def applyInPandas(self,func,schema,batch_size=16000):
      output_schema = schema
      if isinstance(output_schema, str):
        output_schema = schema_str_to_schema(output_schema)
      from snowflake.snowpark.functions import col
      input_types = [x.datatype for x in self._df.schema.fields]
      input_cols  = [x.name for x in self._df.schema.fields]
      output_cols = [x.name for x in output_schema.fields]
      grouping_exprs = [Column(x) for x in self._grouping_exprs]
      clazz=_generate_prefix("applyInPandas")
      def __init__(self):
          self.rows = []
          self.dfs  = []
      def process(self, *row):
          self.rows.append(row)
          # Merge rows into a dataframe
          if len(self.rows) >= batch_size:
             df = pd.DataFrame(self.rows, columns=input_cols)
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
          df = pd.DataFrame(self.rows, columns=input_cols)
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
