from snowflake.snowpark import DataFrame, Row, DataFrameNaFunctions, Column
from snowflake.snowpark.functions import col, lit, udtf, regexp_replace
from snowflake.snowpark import functions as F
from snowflake.snowpark.dataframe import _generate_prefix
from snowflake.snowpark.column import _to_col_if_str
import pandas as pd
from snowpark_extensions.utils import schema_str_to_schema
from snowflake.snowpark.types import StructType,StructField
from snowflake.snowpark._internal.analyzer.expression import FunctionExpression, UnresolvedAttribute
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark import Column
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udtf, col, hash as sf_hash
from snowflake.snowpark.relational_grouped_dataframe import RelationalGroupedDataFrame

from typing import (
    Dict,
    Iterable,
    Optional,
    Union,
    List
)
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    LiteralType,
)
from snowflake.snowpark._internal.utils import generate_random_alphanumeric

from snowflake.snowpark.table_function import (
    TableFunctionCall,
)



if not hasattr(DataFrame,"___extended"):
    
    DataFrame.___extended = True

    def _repr_html_(self):
        import IPython
        rows_limit = getattr(DataFrame,'__rows_limit',50)
        if 'display' in globals():
            display = globals()['display']
        elif 'display' in IPython.get_ipython().user_ns:
            display = IPython.get_ipython().user_ns['display']
        else:
            from IPython.display import display
        try:
            data_to_display = self.limit(rows_limit).collect()
            if len(data_to_display) == 0:
                return "No rows to display"
            else:
                df = pd.DataFrame.from_records([x.as_dict() for x in data_to_display])
                if len(data_to_display) >= rows_limit:
                    print(f"Showing only {rows_limit}. Change DataFrame.__rows_limit value to display more rows")
                display(df)
            return ""
        except Exception as ex:
                return str(ex)

    setattr(DataFrame,'_repr_html_',_repr_html_)
    setattr(DataFrame,'__rows_limit',50)

    def _ipython_key_completions_(self) -> List[str]:
        """Returns the names of columns in this :class:`DataFrame`.
        """
        return self.columns
    setattr(DataFrame,'_ipython_key_completions_',_ipython_key_completions_)

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
            pivot_col = _to_col_if_str(pivot_col,"pivot")
            self.pivot_col=pivot_col
            group_by_exprs = [F.sql_expr(x.sql) for x in old_groupby_col._grouping_exprs]
            group_by_exprs.append(pivot_col)
            # version of snowpark < 1.26.0 stored the dataframe in _df, while later versions store it in _dataframe
            old_dataframe_ref = getattr(old_groupby_col,"_df",None) or getattr(old_groupby_col,"_dataframe",None)
            if old_dataframe_ref is None:
                raise Exception("Cannot find dataframe reference")
            self.df = old_dataframe_ref.groupBy(group_by_exprs)
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
      def prepare(self,prepared_col):
            first = self.df.agg(prepared_col.alias("__firstAggregate"))
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
            if hasattr(self.pivot_col, "_expression") and (isinstance(self.pivot_col._expression, UnresolvedAttribute) or isinstance(self.pivot_col,Alias)):
                colname = self.pivot_col._expression.name
                first = self.df.agg(self.pivot_col)
                self.value_list = [x[0] for x in first.select(self.pivot_col).distinct().sort(self.pivot_col).collect()]
                return self.clean(first.pivot(pivot_col=self.pivot_col,values=self.value_list).agg(F.sql_expr(f"COUNT({colname})")))
            else:
                raise Exception("pivot_col expression is not supported")
      def agg(self, aggregated_col: ColumnOrName) -> DataFrame:
            if hasattr(aggregated_col, "_expression") and isinstance(aggregated_col._expression, FunctionExpression):
                name = aggregated_col._expression.name
                return self.clean(self.prepare(aggregated_col).function(name)(col("__firstAggregate")))
            else:
                raise Exception("Alias functions expressions are supported")
      DataFrame.___original_union_by_name = DataFrame.union_by_name      
      def union_by_name_ext(self, other: "DataFrame", _emit_ast: bool = True, allowMissingColumns:bool = False) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows.

        This method matches the columns in the two DataFrames by their names, not by
        their positions. The columns in the other DataFrame are rearranged to match
        the order of columns in the current DataFrame.

        Example::

            >>> df1 = session.create_dataframe([[1, 2]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[2, 1]], schema=["b", "a"])
            >>> df1.union_by_name(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        if allowMissingColumns:
            self_columns = self.columns
            other_columns = other.columns
            set_self_columns = set(self_columns)
            set_other_columns = set(other.columns)
            # this is done to keep the current order of columns
            common_cols = [x for x in self_columns if x in set_self_columns.intersection(set_other_columns)]
            only_self_columns = set_self_columns - set(common_cols)
            only_other_columns = set_other_columns - set(common_cols)
            TMP_HASH_NAME = "tmp_hash_" + generate_random_alphanumeric()
            if len(only_self_columns) > 0 or len(only_other_columns) > 0:
                self_extented  = self.withColumn(TMP_HASH_NAME, sf_hash("*"))
                other_extended = other.withColumn(TMP_HASH_NAME, sf_hash("*"))

                common_cols.append(TMP_HASH_NAME)
                
                df_combined = self_extented.select(*common_cols).___original_union_by_name(other_extended.select(*common_cols))
                if len(only_self_columns) > 0:
                    df_combined = df_combined.join(self_extented.select(*[x for x in self_extented.columns if x in only_self_columns],TMP_HASH_NAME ), on=TMP_HASH_NAME, how="outer")
                if len(only_other_columns) > 0:
                    df_combined = df_combined.join(other_extended.select(*[x for x in other_extended.columns if x in only_other_columns],TMP_HASH_NAME), on=TMP_HASH_NAME, how="outer")
                return df_combined.drop(TMP_HASH_NAME)            
        return self.___original_union_by_name(other,_emit_ast)
      DataFrame.union_by_name = union_by_name_ext
      DataFrame.unionByName = union_by_name_ext
    def group_by_pivot(self,pivot_col):
        return GroupByPivot(self, pivot_col)
    RelationalGroupedDataFrame.pivot = group_by_pivot

    def transform(self, func, *args, **kwargs):
            result = func(self, *args, **kwargs)
            return result

    DataFrame.transform = transform

    


  ###### HELPER END

  
