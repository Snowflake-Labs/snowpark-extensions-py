from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
import pandas as pd
import html
import traceback

# Ensure we don't double-patch if run multiple times
if not hasattr(DataFrame, "___extended"):
    DataFrame.___extended = True

    # Configuration for default rows
    setattr(DataFrame, '__rows_limit', 20)

    def _repr_html_(self):
        """
        Custom HTML representation for PySpark DataFrames with a rich table UI.
        """
        rows_limit = getattr(DataFrame, '__rows_limit', 20)
        
        try:
            limited_df = self.limit(rows_limit + 1)
            
            timestamp_cols = [
                field.name for field in limited_df.schema.fields
                if isinstance(field.dataType, (TimestampType, DateType))
            ]
            for col_name in timestamp_cols:
                limited_df = limited_df.withColumn(col_name, F.col(col_name).cast("string"))
            
            pdf = limited_df.toPandas()
            
            # Check if dataset is empty
            if pdf.empty:
                return "<div style='font-family: sans-serif; color: #666;'><i>No rows to display</i></div>"

            # 2. Handle truncation
            is_truncated = False
            if len(pdf) > rows_limit:
                pdf = pdf.iloc[:rows_limit]
                is_truncated = True

            # 3. Define the CSS styles
            # Sets distinct light-gray headers, thin borders, and sans-serif font
            styles = [
                dict(selector="table", props=[("border-collapse", "collapse"), ("width", "100%"), ("font-family", '"Helvetica Neue", Helvetica, Arial, sans-serif'), ("font-size", "13px"), ("color", "#333")]),
                dict(selector="th", props=[("background-color", "#f5f5f5"), ("color", "#333"), ("font-weight", "bold"), ("text-align", "left"), ("padding", "8px"), ("border", "1px solid #e3e3e3")]),
                dict(selector="td", props=[("padding", "6px 8px"), ("border", "1px solid #e3e3e3"), ("white-space", "nowrap")]),
                dict(selector="tr:nth-child(even)", props=[("background-color", "#fafafa")]),
                dict(selector="tr:hover", props=[("background-color", "#f1f1f1")]),
            ]

            # 4. Generate Table HTML
            table_html = (pdf.style
                          .set_table_styles(styles)
                          .hide(axis="index") # Hide default pandas index
                          .format(None, na_rep='null') # Display NaN as 'null'
                          .to_html())

            # 5. Build the bottom toolbar
            toolbar_html = f"""
            <div style="background-color: #f5f5f5; border: 1px solid #ddd; border-top: none; padding: 5px; display: flex; align-items: center; gap: 10px;">
                <div style="background-color: #e1e1e1; padding: 4px 8px; border-radius: 3px; cursor: pointer;" title="Table View">
                    <svg width="14" height="14" viewBox="0 0 16 16" fill="#333"><path d="M0 2h16v12H0V2zm2 2v2h3V4H2zm5 0v2h3V4H7zm5 0v2h2V4h-2zM2 8v2h3V8H2zm5 0v2h3V8H7zm5 0v2h2V8h-2z"/></svg>
                </div>
                <div style="padding: 4px 8px; cursor: pointer; color: #666;" title="Chart View (Not active)">
                     <svg width="14" height="14" viewBox="0 0 16 16" fill="#888"><path d="M1 14h14v1H1v-1zm2-5h2v4H3V9zm4-3h2v7H7V6zm4-4h2v11h-2V2z"/></svg>
                </div>
                <div style="font-family: sans-serif; font-size: 11px; color: #888; margin-left: auto;">
                    {f'Showing top {rows_limit} rows' if is_truncated else f'{len(pdf)} rows'}
                </div>
            </div>
            """

            return table_html + toolbar_html

        except Exception as ex:
            full_error = traceback.format_exc()
            return f"<pre style='color:red'>Error displaying DataFrame: {html.escape(full_error)}</pre>"

    # Apply the patches
    setattr(DataFrame, '_repr_html_', _repr_html_)
    setattr(DataFrame, '__rows_limit', 20)

    # Patch key completions for easier column access
    def _ipython_key_completions_(self):
        return self.columns
    setattr(DataFrame, '_ipython_key_completions_', _ipython_key_completions_)

    print("Rich display enabled.")
