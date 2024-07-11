from snowflake.snowpark import Session, DataFrameReader
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, object_construct

if not hasattr(DataFrameReader, "__jdbc_reader__"):
    setattr(DataFrameReader, "__jdbc_reader__", True)
    class JdbcDataFrameReader:
        def __init__(self):
            self.options = {}
        def option(self,key:str,value:str):
            self.options[lit(key)] = lit(value)
            return self
        def query(self,sql:str):
            self.query_stmt = lit(sql)
            return self
        def load(self):
            session = get_active_session()
            jdbc_options = object_construct(*[item for pair in self.options.items() for item in pair])
            return session.table_function("READ_JDBC",jdbc_options,self.query_stmt)
    def format(self,format_name):
            return JdbcDataFrameReader() if format_name == "jdbc" else Exception("not supported")
    def register_jdbc_reader(jdbc_drivers_stage:str, integration_name:str,secrets:str=None):
        from snowflake.snowpark import Session
        from snowflake.snowpark.functions import col
        jdbc_reader_template = """
CREATE OR REPLACE FUNCTION READ_JDBC(OPTION OBJECT, query STRING) 
  RETURNS TABLE(data OBJECT)
  LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = (@@IMPORTS@@)
  EXTERNAL_ACCESS_INTEGRATIONS = (@@EXTERNAL_ACCESS_INTEGRATIONS@@)
  @@SECRETS@@
  HANDLER = 'JdbcDataReader'
AS $$
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;
public class JdbcDataReader {
    public static class OutputRow {
        public Map<String, String> data;
        public OutputRow(Map<String, String> data) {
            this.data = data;
        }
    }
    public static Class getOutputClass() {
      return OutputRow.class;
    }
    public Stream<OutputRow> process(Map<String, String> jdbcConfig, String query) {
        String jdbcUrl = jdbcConfig.get("url");
        String username;
        String password;
        
        if ("true".equals(jdbcConfig.get("use_secrets")))
        {
            SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
            var secret = sfSecrets.getUsernamePassword("cred");
            username   = secret.getUsername();
            password   = secret.getPassword();
        }
        else 
        {
            username = jdbcConfig.get("username");
            password = jdbcConfig.get("password");
        }
        try {
            // Load the JDBC driver 
            Class.forName(jdbcConfig.get("driver"));
            // Create a connection to the database
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            // Create a statement for executing SQL queries
            Statement statement = connection.createStatement();
            // Execute the query
            ResultSet resultSet = statement.executeQuery(query);
            // Get metadata about the result set
            ResultSetMetaData metaData = resultSet.getMetaData();
            // Create a list of column names
            List<String> columnNames = new ArrayList<>();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            // Convert the ResultSet to a Stream of OutputRow objects
            Stream<OutputRow> resultStream = Stream.generate(() -> {
                try {
                    if (resultSet.next()) {
                        Map<String, String> rowMap = new HashMap<>();
                        for (String columnName : columnNames) {
                            String columnValue = resultSet.getString(columnName);
                            rowMap.put(columnName, columnValue);
                        }
                        return new OutputRow(rowMap);
                    } else {
                        // Close resources
                        resultSet.close();
                        statement.close();
                        connection.close();                        
                        return null;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }).takeWhile(Objects::nonNull);
            return resultStream;
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> rowMap = new HashMap<>();
            rowMap.put("ERROR",e.toString());
            return Stream.of(new OutputRow(rowMap));
        }
    }
}
$$;
"""
        session = Session.builder.getOrCreate()
        jars = [f"'@{x[0]}'" for x in session.sql("list @%s" % jdbc_drivers_stage).select(col('"name"')).collect() if x[0].endswith(".jar")]
        secrets_parts = f"SECRETS = ('cred' = {secrets} )" if secrets else ""
        imports = ",".join(jars)
        jdbc_reader_template=jdbc_reader_template.replace("@@IMPORTS@@", imports).replace("@@SECRETS@@",secrets_parts).replace("@@EXTERNAL_ACCESS_INTEGRATIONS@@",integration_name)
        session.sql(jdbc_reader_template).show()
        return "jdbc reader registered"

    DataFrameReader.format = format