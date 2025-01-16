import java.io.File;
import java.sql.*;
import java.time.ZoneOffset;
import java.util.*;


import java.lang.RuntimeException;

import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.TypedAsyncJob;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.*;
import java.security.SecureRandom;
import com.snowflake.snowpark.internal.JavaUtils;

public class JdbcDataReader {
    private volatile Schema avroSchema;
    List<PartitionUtils.Partition> partitions =null;

    private static final SecureRandom randomGenerator = new SecureRandom();
    private static final String TEMP_OBJECT_PREFIX = "JDBC_TEMP_OBJECT";
    private static final int RANDOM_STRING_LENGTH = 8;
    private static Logger logger = LoggerFactory.getLogger(JdbcDataReader.class);

    Configuration conf;
    Connection connection;
    
    public JdbcDataReader() {

    }

    /**
     * Generate a random alphanumeric string of length RANDOM_STRING_LENGTH
     * @param tempObjectType is used tas part of the temp object name
     * @return a string with some random alphanumeric characters
     */
    public static String randomNameForTempObject(String tempObjectType) {

        // Generate a random alphanumeric string of length RANDOM_STRING_LENGTH
        StringBuilder randStr = new StringBuilder();
        String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        for (int i = 0; i < RANDOM_STRING_LENGTH; i++) {
            randStr.append(alphanumeric.charAt(randomGenerator.nextInt(alphanumeric.length())));
        }

        // Combine prefix, type, and random string to form the name
        String name = String.format("%s_%s_%s", TEMP_OBJECT_PREFIX, tempObjectType, randStr);

        return name;
    }

    /**
     * Hadoop default logging is too extensible and can cause a great deal of overhead.
     * So we disable some of this logging to achieve better performance.
     */
    private static void disableHadoopLogging() throws Exception
    {
        try {
            // Logger names
            String[] loggers = {
                "org.apache.parquet",
                "org.apache.hadoop.fs.FileSystem",
                "org.apache.parquet.io.MessageColumnIO",
                "org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreImpl",
                "org.apache.hadoop.metrics2.lib.MutableMetricsFactory",
                "org.apache.hadoop.metrics2.impl.MetricsSystemImpl",
                "org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration"
            };
            // Load the Level class dynamically
            Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
        
            // Load the Level class dynamically
            // Access the static WARN field
            Field warnField = levelClass.getField("OFF");
            Object warnLevel = warnField.get(null); // Retrieve the static value of OFF


            // Iterate over the loggers to set their level to OFF
            for (String loggerName : loggers) {
                // Get the logger instance
                Logger logger = LoggerFactory.getLogger(loggerName);

                // Use reflection to access the `setLevel` method
                Class<?> loggerClass = logger.getClass();
                Method setLevelMethod = loggerClass.getMethod("setLevel", levelClass);

                // Call setLevel with WARN
                setLevelMethod.invoke(logger, warnLevel);

                System.out.println("Log level for " + loggerName + " set to OFF");
            }
        } catch (Exception e) {
            throw e;
        }        
    }



    /**
     * Build an Schema object from a ResultSetMetaData object
     * @param metaData JDBC Metadata object
     * @return Avro Schema object
     * @throws SQLException
     */
    public Schema getAvroSchema(ResultSetMetaData metaData) throws SQLException {
        if (avroSchema == null) { // First check (non-blocking)
            synchronized (Schema.class) {
                if (avroSchema == null) { // Second check (thread-safe initialization)
                    String avroSchemaStr = buildSchemaString(metaData);
                    avroSchema = new Schema.Parser().parse(avroSchemaStr);
                }
            }
        }
        return avroSchema;
    }


    /**
     * Generate a file name based on a prefix and a UUID
     * @param prefix
     * @return
     */
    public String generateFileName(String prefix) {
        UUID guid = UUID.randomUUID();
        return String.format("/tmp/%s_%s.parquet", prefix, guid.toString());
    }
   
    /**
     * Build a string representation of an Avro schema from a ResultSetMetaData object
     * @param metaData
     * @return
     * @throws SQLException
     */
    public static String buildSchemaString(ResultSetMetaData metaData) throws SQLException {
        // Start building the schema string
        StringBuilder schemaStringBuilder = new StringBuilder();
        schemaStringBuilder.append("{\n")
            .append("    \"type\": \"record\",\n")
            .append("    \"name\": \"User\",\n")
            .append("    \"fields\": [\n");
        // Get the number of columns in the ResultSetMetaData
        int columnCount = metaData.getColumnCount();
        // Iterate over the columns and build the fields part of the schema
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            // Handle columns with no name
            if (columnName == null || columnName.isEmpty())
                columnName = "column" + i;
            // Get the column type
            String columnType = JDBCSqlTypeToAvroType(metaData, i);
            // And handle nullability
            if (metaData.isNullable(i) == ResultSetMetaData.columnNoNulls)
                columnType = "\"type\": " + columnType;
            else 
                columnType = "\"type\": [\"null\"," + columnType + "]";
            schemaStringBuilder.append(" {\"name\": \"")
            .append(columnName).append("\", ")
            .append(columnType).append("}");
            // Add a comma if it's not the last column
            if (i < columnCount) schemaStringBuilder.append(",");
            schemaStringBuilder.append("\n");
        }
        // End the schema string
        schemaStringBuilder.append("    ]\n").append("}");
        return schemaStringBuilder.toString();
    }


    /**
     * Helper method to map SQL types to Avro types
     * @param metaData JDBC metadata object
     * @param i index of the column
     * @returna string json string representation of the avro type
     * @throws SQLException
     */
    private static String JDBCSqlTypeToAvroType(ResultSetMetaData metaData, int i) throws SQLException {
        int sqlType = metaData.getColumnType(i);
        switch (sqlType) {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR:
                return "\"string\" ";
            case java.sql.Types.INTEGER:
                return "\"int\" ";
            case java.sql.Types.BIGINT:
                return "\"long\" ";
            case java.sql.Types.DOUBLE:
                return "\"double\" ";
            case java.sql.Types.FLOAT:
                return "\"float\" ";
            case java.sql.Types.BOOLEAN:
                return "\"boolean\" ";
            case java.sql.Types.DATE:
                return "{\"type\": \"int\", \"logicalType\": \"date\"}";
            case java.sql.Types.TIMESTAMP:
                return "{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}";
            case java.sql.Types.DECIMAL:
                var precision = metaData.getPrecision(i);
                var precisionStr = Integer.toString(precision);
                var scale = metaData.getScale(i);
                var scaleStr = Integer.toString(scale);
                return "{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": " + precisionStr +", \"scale\": " + scaleStr +"}";
            default:
                return "\"string\" ";
        }
    }

    /**
     * Execute the given SQL query, saves into parquet and return the path of the parquet file
     * @param query SQL query to execute
     * @return path to parquet file
     * @throws SQLException
     * @throws IOException
     */
    public String executeQuery(String query) throws SQLException, IOException {
        // Create a statement for executing SQL queries
        Statement statement = connection.createStatement();
        // Execute the query
        long start = System.currentTimeMillis();
        ResultSet resultSet = statement.executeQuery(query);
        long end = System.currentTimeMillis();
        logger.info("Query Elapsed time in milliseconds: " + (end - start));

        // Get metadata about the result set
        ResultSetMetaData metaData = resultSet.getMetaData();
        getAvroSchema(metaData);
        // Create a list of column names
        List<String> columnNames = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            var columnName = metaData.getColumnLabel(i);
            // handle columns with no name
            if (columnName == null || columnName.isEmpty()) {
                columnName = "column" + i;
            }
            columnNames.add(columnName);
        }

        // Output Parquet file
        var filename = generateFileName("data");
        File outputFile = new File(filename);

        // Create Parquet writer
        long startWrite = System.currentTimeMillis();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath()))
                .withSchema(avroSchema)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withConf(this.conf)
                .build()) {
                while (resultSet.next()) {
                    GenericRecord rowMap = new GenericData.Record(avroSchema);
                    for (var i=0;i<columnCount;i++)
                    {
                        Object columnValue = resultSet.getObject(i+1);
                        if (columnValue instanceof java.time.LocalDateTime) {
                            columnValue = Long.valueOf( 
                                ((java.time.LocalDateTime)columnValue).toInstant(ZoneOffset.UTC).toEpochMilli());
                        }
                        else if (columnValue instanceof java.util.Date) {
                            columnValue = Long.valueOf(((java.util.Date)columnValue).getTime());
                        }
                        rowMap.put(columnNames.get(i), columnValue);
                    }
                    writer.write(rowMap);
                }
        }
        long endWrite = System.currentTimeMillis();
        logger.info("Write Elapsed time in milliseconds: " + (endWrite - startWrite));
        return filename;
    }

    /**
     * Execute the query in partitions (experimental)
     * @param session
     * @param jdbcConfig
     * @param query
     * @return
     * @throws Exception
     */
    private String executeInPartitions(Session session, Map<String, String> jdbcConfig, String query) throws Exception {
        logger.info("starting execution in partitions");
        if (!jdbcConfig.containsKey("lowerBound") || jdbcConfig.get("lowerBound") == null || jdbcConfig.get("lowerBound").isEmpty()) {
            throw new IllegalArgumentException("The 'partitionColumn' property should be provided with 'lowerBound' property.");
        }
        if (!jdbcConfig.containsKey("upperBound") || jdbcConfig.get("upperBound") == null || jdbcConfig.get("upperBound").isEmpty()) {
            throw new IllegalArgumentException("The 'upperBound' property should be provided with 'lowerBound' property.");
        }

        String stageName   = randomNameForTempObject("STAGE");
        String formatName  = randomNameForTempObject("FORMAT");
        String tableName   = randomNameForTempObject("TABLE");
        session.sql("CREATE OR REPLACE TEMP STAGE " + stageName).collect();
        jdbcConfig.put("sf_stage_location", stageName);
        logger.info("execution in partitions will use stage: " + stageName);
        Map<String,String> partitioningInfoMap = new HashMap<>();
        partitioningInfoMap.put("partitionColumn", jdbcConfig.get("partitionColumn"));
        partitioningInfoMap.put("lowerBound", jdbcConfig.get("lowerBound"));
        partitioningInfoMap.put("upperBound", jdbcConfig.get("upperBound"));
        partitioningInfoMap.put("numPartitions", jdbcConfig.get("numPartitions"));
        jdbcConfig.remove("partitionColumn");
        jdbcConfig.remove("lowerBound");
        jdbcConfig.remove("upperBound");
        jdbcConfig.remove("numPartitions");    
        var partitions = PartitionUtils.columnPartition(partitioningInfoMap);
        List<TypedAsyncJob<Row[]>> asyncActors = new ArrayList<>();
        for (var partition : partitions) {
            logger.info("Starting partition: " + partition.getPartitionId());
            var partitionQuery = "select * from (" + query + ") queryAlias where " + partition.getWhereClause();
            var callQuery = "CALL READ_JDBC2(" + asVariantString(jdbcConfig) + ", '" + partitionQuery + "')";
            var asyncCollect = session.sql(callQuery).async().collect();
            asyncActors.add(asyncCollect);
        }
        logger.info("Waiting for partitions to complete...");
        for (var asyncActor : asyncActors) asyncActor.getResult();
        logger.info("Completed waiting for partitions to complete.");
        logger.info("Creating stage and format...");
        session.sql("CREATE OR REPLACE FILE FORMAT " + formatName + " TYPE = PARQUET").collect();
        
        logger.info("complete creating stage and format");
        logger.info("Creating table for all the partitions results...");
        session.sql(
            "CREATE OR REPLACE TABLE " + tableName +
            " USING TEMPLATE ( " +
            " SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) " +
            "    FROM TABLE( "+
            "      INFER_SCHEMA( " +
            "        LOCATION=>'@" + stageName + "', " +
            "        FILE_FORMAT=>'" + formatName + "' " +
            "       ) " + 
            "     )) ").collect();
        logger.info("Completed creating table for all the partitions results.");
        session.sql("copy into " + tableName + " from @" + stageName + " FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE").collect();    
        logger.info("Completed loading data from partitions from stage into table.");
        return tableName;
    }

    private String asVariantString(Map<String, String> jdbcConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean firstItem = true;
        for (var key : jdbcConfig.keySet())
        {
            if (firstItem)
                firstItem = false;
            else
                sb.append(",");
            sb.append("'" + key + "'").append(":").append("'" + jdbcConfig.get(key) + "'");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Establishes a connection to the JDBC source and executes the query to save the results into a parquet file.
     * @param session
     * @param jdbcConfig
     * @param query
     * @return
     * @throws Exception
     */
    public String saveQueryIntoFile(Session session, Map<String, String> jdbcConfig, String query) throws Exception {
        logger.info("Executing query: " + query);
        disableHadoopLogging();
        this.conf = getHadoopConf();        
        Properties properties = validateAndAdjustOptions(jdbcConfig);
        properties.putAll(jdbcConfig);
        String jdbcUrl = jdbcConfig.get("url");
        logger.info("Used properties: " + properties.toString());
        try {
            // Load the JDBC driver 
            Class.forName(jdbcConfig.get("driver"));
            // Create a connection to the database
            this.connection = DriverManager.getConnection(jdbcUrl, properties);
            return executeQuery(query);
        } catch (Throwable e) {
            logger.error("Error while writing to file: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Validate the JDBC options and adjust them if necessary.
     * @param jdbcConfig
     * @return
     */
    private Properties validateAndAdjustOptions(Map<String, String> jdbcConfig) {
        Properties properties = new Properties();
        // Check for required properties
        if (!jdbcConfig.containsKey("url") || jdbcConfig.get("url") == null || jdbcConfig.get("url").isEmpty()) {
            throw new IllegalArgumentException("The 'url' property is required and cannot be null or empty.");
        }

        if (!jdbcConfig.containsKey("driver") || jdbcConfig.get("driver") == null || jdbcConfig.get("driver").isEmpty()) {
            throw new IllegalArgumentException("The 'driver' property is required and cannot be null or empty.");
        }

        if (jdbcConfig.containsKey("sf_secret"))
        {
            // use secret alias to retrieve credentials
            String secret_alias = jdbcConfig.get("sf_secret");
            try {
                SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
                jdbcConfig.remove("sf_secret");
                var user_and_password = sfSecrets.getUsernamePassword(secret_alias);
                properties.setProperty("user",     user_and_password.getUsername());
                properties.setProperty("password", user_and_password.getPassword());
            } 
            catch (Exception e) {
                logger.error("Error loading secrets", e);
                throw new RuntimeException(new Error("Failed to load secrets for secret alias: " + secret_alias));
            }
        }
        return properties;
    }

    private Configuration getHadoopConf() {
        var conf = new Configuration();
        FileSystem fs = null;
        try {
            conf.set("fs.file.impl.disable.cache","true");
            conf.set("fs.file.impl", RawLocalFileSystemPatched.class.getName());
            fs = FileSystem.get(conf);
        } catch (IOException ex) { 
            throw new RuntimeException(ex);
        }
        return conf;
    }
    
    public String proc_main(com.snowflake.snowpark_java.Session session,Map<String, String> options, String query) throws Exception {
        if (options.containsKey("partitionColumn")) {
            return executeInPartitions(session, options, query);
        }
        String stageName   = randomNameForTempObject("STAGE");
        String formatName  = randomNameForTempObject("FORMAT");
        String tableName   = randomNameForTempObject("TABLE");
        boolean existing = false;
        if (options.containsKey("sf_stage_location")) {
            
            // if a stage location is provide that means that we will use that stage location
            // but we wont we creating tables or formats as 
            // ingestion wont be done
            stageName = options.get("sf_stage_location");
            logger.info("Using existing stage location >>>>>> : " + stageName );
            existing = true;
        }
        if (!existing) {
            session.sql("CREATE OR REPLACE FILE FORMAT " + formatName + " TYPE = PARQUET").collect();
            session.sql("CREATE OR REPLACE STAGE " + stageName).collect();
        }
        
        var generateFileName = new JdbcDataReader().saveQueryIntoFile(session, options, query);
        // Copy the generated file to the stage
        Map<String, String> stage_options = new HashMap<>();
        stage_options.put("AUTO_COMPRESS", "FALSE");
        session.file().put(generateFileName,"@" + stageName,stage_options);
        if (!existing) {
            session.sql(
            "CREATE OR REPLACE TABLE " + tableName +
            " USING TEMPLATE ( " +
            " SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) " +
            "    FROM TABLE( "+
            "      INFER_SCHEMA( " +
            "        LOCATION=>'@" + stageName + "', " +
            "        FILE_FORMAT=>'" + formatName + "' " +
            "       ) " + 
            "     )) ").collect();
            session.sql("copy into " + tableName + " from @" + stageName + " FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE").show();    
        }
        return tableName;
    }

}