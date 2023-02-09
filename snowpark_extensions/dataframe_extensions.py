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
from snowflake.snowpark import Window, Column
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udtf, col
from snowflake.snowpark.relational_grouped_dataframe import RelationalGroupedDataFrame

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

    def stack(self,rows:int,*cols):
        count_cols = len(cols)
        if count_cols % rows != 0:
            raise Exception("Invalid parameter. The given cols cannot be arrange in the give rows")
        out_count_cols = int(count_cols / rows)
        # determine the input schema
        input_schema = self.select(cols).limit(1).schema
        from snowflake.snowpark.functions import col
        input_types = [x.datatype for x in input_schema.fields]
        input_cols  = [x.name     for x in input_schema.fields]
        output_cols = [f"col{x}" for x in range(1,out_count_cols)]
        clazz=_generate_prefix("stack")
        def process(self, *row):
            for i in range(0, len(row), out_count_cols):
                yield tuple(row[i:i+out_count_cols])
        output_schema = StructType([StructField(f"col{i+1}",input_schema.fields[i].datatype) for i in range(0,out_count_cols)])
        udtf_class = type(clazz, (object, ), {"process":process})
        tfunc = udtf(udtf_class,output_schema=output_schema, input_types=input_types,name=clazz,replace=True,is_permanent=False,packages=["snowflake-snowpark-python"])
        return tfunc(*cols)
    
    DataFrame.stack = stack

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

    def group_by_pivot(self,pivot_col):
        return GroupByPivot(self, pivot_col)
    RelationalGroupedDataFrame.pivot = group_by_pivot

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
