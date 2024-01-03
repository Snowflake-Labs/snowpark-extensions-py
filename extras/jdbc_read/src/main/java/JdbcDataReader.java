import java.io.PrintWriter;
import java.io.StringWriter;
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
        Properties properties = new Properties();

        if ("true".equals(jdbcConfig.get("use_secrets")))
        {
            SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
            jdbcConfig.remove("use_secrets");
            var secret = sfSecrets.getUsernamePassword("cred");
            properties.setProperty("username", secret.getUsername());
            properties.setProperty("password", secret.getPassword());
        }
    
        try {
            properties.putAll(jdbcConfig);
            // Load the JDBC driver 
            Class.forName(jdbcConfig.get("driver"));
            // Create a connection to the database
            Connection connection = DriverManager.getConnection(jdbcUrl, properties);
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
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();
            Map<String, String> rowMap = new HashMap<>();
            rowMap.put("ERROR",e.getMessage() + "\n" + stackTrace);
            return Stream.of(new OutputRow(rowMap));
        }
    }
}