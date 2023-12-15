# PMML in Snowpark

Predictive Model Markup Language ([PMML](https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language)) is an open XML-based predictive model interchange format.

In order to easily use these models in SnowPark a simple helper has been provided.

NOTE: this helper requires some JAR in order to perform the PMML loading and scoring.

These libraries can be downloaded from maven and they should be uploaded into an stage:

```
 https://mvnrepository.com/artifact/org.pmml4s/pmml4s
 https://repo1.maven.org/maven2/org/pmml4s/pmml4s_2.12/1.0.1/pmml4s_2.12-1.0.1.jar

 https://mvnrepository.com/artifact/io.spray/spray-json 
 https://repo1.maven.org/maven2/io/spray/spray-json_2.12/1.3.2/spray-json_2.12-1.3.6.jar

 https://mvnrepository.com/artifact/org.scala-lang/scala-library
 https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.17/scala-library-2.12.17.jar
```

And to use it in your code you can use an snippet like:

```
from pmml_builder import ScoreModelBuilder
scorer = ScoreModelBuilder() \
.fromModel("MYDB.MYSCHEMA.MYSTAGE/dt-stroke.pmml") \
.withSchema(df.schema) \
.withStageLibs("MYDB.MYSCHEMA_FOR_LIBS.STAGE_FOR_LIBS") \
.build()

scorer.transform(df).show()

```

# NOTE:

In some scenarios the number for input parameters or the number of output parameters can vary, in order to simplify the usage in those situations we provide a [helper UDF ](https://github.com/Snowflake-Labs/snowpark-extensions-py/blob/main/extras/pmml/PMML_SCORER.sql)

To use that helper you will need to previously upload the:

* SCALA_LIB for example `scala-library-2.12.17.jar` into an stage, you can [check maven](https://mvnrepository.com/artifact/org.scala-lang/scala-library)
* PMMLS_LIB for example `pmml4s_2.12-1.0.1.jar` into an stage, you can [check maven](https://mvnrepository.com/artifact/org.pmml4s/pmml4s)
* SPRAY_LIB for example `spray-json_2.12-1.3.6.jar` into an stage, you can [check maven](https://mvnrepository.com/artifact/io.spray/spray-json)

To use this in python it will be easy to do:

```python
table_with_data
.select(sql_expr("object_construct(*)").alias("INPUT_DATA"))
.join_table_function("PMML_SCORER",lit("@STAGE/path/to/model"),col("INPUT_DATA"))
```