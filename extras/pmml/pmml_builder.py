# This helper requires some jars to be previously uploaded to an accesible stage
# https://mvnrepository.com/artifact/org.pmml4s/pmml4s
# https://repo1.maven.org/maven2/org/pmml4s/pmml4s_2.12/1.0.1/pmml4s_2.12-1.0.1.jar

# https://mvnrepository.com/artifact/io.spray/spray-json 
# https://repo1.maven.org/maven2/io/spray/spray-json_2.12/1.3.2/spray-json_2.12-1.3.6.jar

# https://mvnrepository.com/artifact/org.scala-lang/scala-library
# https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.17/scala-library-2.12.17.jar

import json
from snowflake.snowpark.session import Session,DataFrame, TableFunctionCall, Column
from snowflake.snowpark.functions import col, table_function
from snowflake.snowpark.types import *
from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type
from snowflake.snowpark._internal.utils import generate_random_alphanumeric

SCALA_LIB="scala-library-2.12.17.jar"
PMMLS_LIB="pmml4s_2.12-1.0.1.jar"
SPRAY_LIB="spray-json_2.12-1.3.6.jar"

def get_udf_code(prefix:str,stage_libs:str,model_file_name:str,sql_args:str, input_args:str,java_call_args,java_args):
    import os
    just_filename_model_file_name = os.path.basename(model_file_name)
    return f"""
    CREATE OR REPLACE TEMPORARY FUNCTION PMML_SCORER_{prefix}({sql_args})
    RETURNS TABLE(score VARIANT)
    LANGUAGE JAVA
    RUNTIME_VERSION = 11
    IMPORTS = ('@{model_file_name}', '@{stage_libs}/{PMMLS_LIB}','@{stage_libs}/{SPRAY_LIB}','@{stage_libs}/{SCALA_LIB}')
    PACKAGES = ('com.snowflake:snowpark:latest')
    handler='PMMLModel'
    target_path='@~/pmmlscorer_{prefix}.jar'
    AS '
    import com.snowflake.snowpark_java.types.*;
    import java.util.List;
    import java.util.Arrays;
    import java.util.stream.Stream;
    import org.pmml4s.model.Model;
    public class PMMLModel  
    {{
        Model model;
        public PMMLModel() {{
            String fPath = System.getProperty("com.snowflake.import_directory") + "{just_filename_model_file_name}";
            this.model = Model.fromFile(fPath);
        }}

        public class OutputRow {{
            public Variant SCORE;
            public OutputRow(Variant SCORE) {{ this.SCORE = SCORE;}}
        }}

        public StructType outputSchema()  {{ return StructType.create(new StructField("SCORE",  DataTypes.VariantType)); }}

        public static Class<?> getOutputClass() {{  return OutputRow.class; }}

        public StructType inputSchema() {{
            return StructType.create(
               {input_args}
            );
        }}

        public Stream<OutputRow> endPartition() {{ return Stream.empty(); }}

        public Stream<OutputRow> process({java_call_args}) 
        {{
            Object[] arg_array = {{ {java_args} }};
            var results = this.model.predict(Arrays.asList(arg_array));
            return Stream.of(new OutputRow(new Variant(results)));  
        }}
    }}';
"""


class ScoreModel():
    def __init__(self, session:Session,scorer:TableFunctionCall,columns:List[Column]):
        self.session = session
        self.scorer = scorer
        self.columns = columns
    def transform(self,df:DataFrame):
        from snowflake.snowpark.functions import col
        return df.join_table_function(self.scorer (*self.columns))

class ScoreModelBuilder():


    def __init__(self):
        self.model_location = None
        self.df = None
        self.stagelib = None
        self.session = None
    def to_java_datatype(self,field_datatype) -> str:
        return str(field_datatype).replace("()","")


    def to_java(self,data_type):
        if isinstance(data_type, StringType):
            return "String"
        elif isinstance(data_type, IntegerType):
            return "int"
        elif isinstance(data_type, LongType):
            return "long"
        elif isinstance(data_type, DoubleType):
            return "double"
        elif isinstance(data_type, FloatType):
            return "float"
        elif isinstance(data_type, DecimalType):
            return "java.math.BigDecimal"
        elif isinstance(data_type, BooleanType):
            return "boolean"
        elif isinstance(data_type, TimestampType):
            return "java.sql.Timestamp"
        elif isinstance(data_type, DateType):
            return "java.sql.Date"
        elif isinstance(data_type, ArrayType):
            element_type = self.to_java(data_type.elementType)
            return "java.util.List<{}>".format(element_type)
        elif isinstance(data_type, StructType):
            fields = []
            for field in data_type.fields:
                field_type = self.to_java(field.dataType)
                fields.append("    {} {},".format(field_type, field.name))
            return "public class {} {{\n{}\n}}".format(data_type.simpleString(), "\n".join(fields))
        else:
            raise ValueError("Unsupported data type: {}".format(data_type))
   
    def fromModel(self,model_location:str):
        self.model_location = model_location
        return self
    def withSchema(self,schema):
        self.schema = schema
        return self
    def withStageLibs(self,stage:str):
        self.stagelib = stage
        return self
    def withSession(self,session:Session):
        self.session = session
        return self
    def build(self):
        if(self.stagelib == None):
            raise Exception("Missing stage lib")
        if(self.model_location == None):
            raise Exception("Missing model location")
        if(self.schema == None):
            raise Exception("Missing schema")
        
        if not self.session:
            from snowflake.snowpark.context import get_active_session
            self.session = get_active_session()
        java_args_names = [x.name for x in self.schema.fields]
        java_args_types  = [self.to_java(x.datatype) for x in self.schema.fields]
        java_args_dtype = [self.to_java_datatype(x.datatype) for x in self.schema.fields]
        input_args_schema = ",\n".join([f'new StructField("{x[0]}", DataTypes.{x[1]})'  for x in zip(java_args_names,java_args_dtype)])
        #scala_args_types = [map_python_to_scala(x[1]) for x in schema_info]
        sql_args_types = [convert_sp_to_sf_type(x.datatype) for x in self.schema.fields]
        sql_args_str = ",".join([f'{x[0]} {x[1]}'  for x in zip(java_args_names,sql_args_types)])
        num_args = len(self.schema.fields)
        java_args_decl = ", ".join([f"{x[1]} {x[0]}" for x in zip(java_args_names,java_args_types)])
        #input_schema_fields = ", ".join([f"""StructType("{x[0]}",{x[1]})""" for x in zip(java_args_names,java_args_types)])
        self.prefix = generate_random_alphanumeric(4)
        self.func_name = "PMML_SCORER_" + self.prefix
        self.udf_code = get_udf_code(self.prefix,self.stagelib, self.model_location, sql_args_str,input_args_schema, java_args_decl, ", ".join(java_args_names))
        print(self.udf_code)
        self.session.sql(self.udf_code).show()
        self.scorer = table_function(self.func_name)
        self.columns = [col(x) for x in java_args_names]
        return ScoreModel(self.session, self.scorer, self.columns)
