# PDF generation helpers

Sometimes you might need to do some PDF generation in Snowpark.

We provide some small snippets to ease some common PDF tasks.

To use this class:

```
from reporting import PDFReport
newdoc=PDFReport()

```

If you need to add some text paragraphs:

```
newdoc.add_paragraph("some <b>text</b> ")
newdoc.add_paragraph("""Lorem <b>ipsum dolor</b> sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. """)
```

If you need to add a table with some data:

```
from snowflake.snowpark import Session
session = Session.builder.getOrCreate()
df = session.sql("select C_NAME, C_PHONE, C_ADDRESS from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER limit 10").to_pandas()
newdoc.add_table(df)

```

And you can insert new page breaks:

```
newdoc.pagebreak()
```

Finally, you can save the document:

```
newdoc.write_pdfpage()
```

That will save the report in `/tmp/document.pdf`