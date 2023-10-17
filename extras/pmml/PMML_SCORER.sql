 
-- You might need to adjust this libraries for newer version as well as the stage
-- SCALA_LIB="scala-library-2.12.17.jar"
-- PMMLS_LIB="pmml4s_2.12-1.0.1.jar"
-- SPRAY_LIB="spray-json_2.12-1.3.6.jar"

 CREATE OR REPLACE FUNCTION SNOWPARK_TESTDB.PUBLIC.PMML_SCORER(MODEL_URL STRING,ARGS OBJECT)
    RETURNS TABLE(score VARIANT)
    LANGUAGE JAVA
    RUNTIME_VERSION = 11
    IMPORTS = ('@TEST/pmml4s_2.12-1.0.1.jar','@TEST/spray-json_2.12-1.3.6.jar','@TEST/scala-library-2.12.17.jar')
    PACKAGES = ('com.snowflake:snowpark:latest')
    handler='PMMLModel'
    target_path='@~/pmmlscorer.jar'
    AS $$
    import com.snowflake.snowpark_java.types.*;
    import java.util.*;
    import java.util.stream.Stream;
    import org.pmml4s.model.Model;
    import org.pmml4s.util.Utils;
    //import org.pmml4s.common.StructType;
    //import org.pmml4s.common.StructField;
    public class PMMLModel  
    {
        Map<String,Model> models;
        public PMMLModel() {
            this.models = new HashMap<String,Model>();
        }

        public class OutputRow {
            public Variant SCORE;
            public OutputRow(Variant SCORE) { this.SCORE = SCORE;}
        }

        public StructType outputSchema(){ 
            return StructType.create(
                new StructField("SCORE", DataTypes.VariantType)); 
        }

        public static Class<?> getOutputClass() {  return OutputRow.class; }

        public StructType inputSchema() {
            return StructType.create(
                new StructField("MODEL_URL", DataTypes.StringType),
                new StructField("ARGS",      DataTypes.VariantType)
            );
        }

        public Stream<OutputRow> endPartition() { return Stream.empty(); }

        public Model getModel(String MODEL_URL) {
            Model model = models.get(MODEL_URL);
            if (model == null) { 
                SnowflakeFile file = SnowflakeFile.newInstance(MODEL_URL,false); 
                // the false argument is so we dont need to pass scoped url
                model = Model.fromInputStream(file.getInputStream());
                models.put(MODEL_URL, model);
            }
            return model;
        }

        public Object[] asArray(Model model, Map<String,String> ARGS) {
            var inputSchema = model.inputSchema();            
            Object[] values = new Object[inputSchema.size()];
            for (int i = 0; i < values.length; i++) {
                var sf = inputSchema.apply(i);
                var fieldValue = ARGS.get(sf.name());
                try {
                    values[i] = Utils.toVal(fieldValue,sf.dataType());
                } catch (Exception e) {
                    values[i] = fieldValue;
                }
            }
            return values;
        }

 

        public Stream<OutputRow> process(String MODEL_URL, Map<String,String> ARGS) 
        {
            var model = this.getModel(MODEL_URL);
            var result = model.predict(asArray(model, ARGS));
            return Stream.of(new OutputRow(new Variant(result)));  
        }
    }
    $$;

-- the function can be used as follows:
    SELECT S.* EXCLUDE ARGS, SCORE from 
    (SELECT *, OBJECT_CONSTRUCT(*) ARGS FROM SNOWPARK_TESTDB.PUBLIC.STROKE) S,
    TABLE(SNOWPARK_TESTDB.PUBLIC.PMML_SCORER('@SNOWPARK_TESTDB.PUBLIC.TEST/dt-stroke.pmml',S.ARGS));
