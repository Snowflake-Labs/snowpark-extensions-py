import json
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
from pmml_builder import ScoreModelBuilder
from pathlib import Path
import os
# get the directory of the current Python script
script_dir = Path(__file__).resolve().parent

with open(f"{script_dir}{os.sep}creds.json") as f:
    data = json.load(f)
    username = data['username']
    password = data['password']
    account = data["account"]
    warehouse = data["warehouse"]
    database = data["database"]
    schema = data["schema"]
    role = data["role"]

CONNECTION_PARAMETERS = {
    'account': account,
    'user': username,
    'password': password,
    'schema': schema,
    'database': database,
    'warehouse': warehouse,
    "role": role
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()    


data_stage_name = "DATA_STAGE"
data_file_path = f"{script_dir}{os.sep}train_strokes.csv"
session.sql(f"CREATE or REPLACE STAGE {data_stage_name}").collect()
print('Putting '+ data_file_path +' to stage: '+data_stage_name)
session.file.put(local_file_name=data_file_path, 
                     stage_location=data_stage_name + '/stroke', 
                     source_compression='NONE', 
                     overwrite=True)

model_stage_name = "MODEL_STAGE"
model_file_path = f"{script_dir}{os.sep}dt-stroke.pmml"
session.sql(f"CREATE or REPLACE STAGE {model_stage_name}").collect()

print('Putting '+ model_file_path +' model_stage_name: '+model_stage_name)
session.file.put(local_file_name=model_file_path, 
                     stage_location=model_stage_name, 
                     source_compression='NONE', 
                     overwrite=True)

load_schema = T.StructType([T.StructField("ID", T.IntegerType()),
                             T.StructField("GENDER", T.StringType()), 
                             T.StructField("AGE", T.IntegerType()), 
                             T.StructField("HYPERTENSION", T.IntegerType()),
                             T.StructField("HEART_DISEASE", T.IntegerType()),
                             T.StructField("EVER_MARRIED", T.StringType()),
                             T.StructField("WORK_TYPE", T.StringType()),
                             T.StructField("RESIDENCE_TYPE", T.StringType()),
                             T.StructField("AVG_GLUCOSE_LEVEL", T.FloatType()),
                             T.StructField("BMI", T.StringType()),
                             T.StructField("SMOKING_STATUS", T.StringType()),
                             T.StructField("STROKE", T.IntegerType())
                        ])

table_name = "STROKE"

session.create_dataframe([[None]*len(load_schema.names)], schema=load_schema)\
       .na.drop()\
       .write\
       .mode("overwrite") \
       .save_as_table(table_name)

csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}

loadresults = session.read.option("SKIP_HEADER", 1)\
                     .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
                     .option("COMPRESSION", "GZIP")\
                     .option("NULL_IF", "\\\\N")\
                     .option("NULL_IF", "NULL")\
                     .schema(load_schema)\
                     .csv('@'+data_stage_name + '/stroke/')\
                     .copy_into_table(table_name, format_type_options=csv_file_format_options)

df = session.table("stroke")
scorer = ScoreModelBuilder() \
.fromModel("MODEL_STAGE/dt-stroke.pmml") \
.withSchema(df.schema) \
.withStageLibs("SNOWPARK_TESTDB.PUBLIC.TEST") \
.build()

# You will get an output like:
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# |"ID"   |"GENDER"  |"AGE"  |"HYPERTENSION"  |"HEART_DISEASE"  |"EVER_MARRIED"  |"WORK_TYPE"    |"RESIDENCE_TYPE"  |"AVG_GLUCOSE_LEVEL"  |"BMI"  |"SMOKING_STATUS"  |"STROKE"  |"SCORE"                 |
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# |41413  |Female    |75     |0               |1                |Yes             |Self-employed  |Rural             |243.53               |27     |never smoked      |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8225806451612904,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.1774193548387097    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |25774  |Male      |35     |0               |0                |No              |Private        |Rural             |85.37                |33     |never smoked      |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8225806451612904,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.1774193548387097    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |4057   |Male      |71     |0               |0                |Yes             |Private        |Urban             |198.21               |27.3   |formerly smoked   |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8484848484848485,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.15151515151515152   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |29908  |Female    |47     |0               |0                |Yes             |Private        |Urban             |103.26               |25.4   |NULL              |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8484848484848485,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.15151515151515152   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |70970  |Female    |17     |0               |0                |No              |Self-employed  |Urban             |82.18                |23.4   |NULL              |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  1,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.35714285714285715,  |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.6428571428571429    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |58261  |Female    |66     |0               |0                |Yes             |Private        |Rural             |141.24               |28.5   |never smoked      |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8484848484848485,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.15151515151515152   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |41648  |Male      |27     |0               |0                |Yes             |Private        |Rural             |102.64               |26.4   |smokes            |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  1,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.35714285714285715,  |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.6428571428571429    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |9046   |Male      |67     |0               |1                |Yes             |Private        |Urban             |228.69               |36.6   |formerly smoked   |1         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.8484848484848485,   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.15151515151515152   |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |1686   |Female    |29     |0               |0                |No              |Private        |Urban             |71.89                |27.6   |never smoked      |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  1,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.35714285714285715,  |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.6428571428571429    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# |65535  |Male      |8      |0               |0                |No              |children       |Rural             |78.05                |25.7   |NULL              |0         |[                       |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  1,                    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.35714285714285715,  |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |  0.6428571428571429    |
# |       |          |       |                |                 |                |               |                  |                     |       |                  |          |]                       |
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

scorer.transform(df).show()
print("done")