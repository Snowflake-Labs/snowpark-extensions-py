# JDBC Data Reader for Snowpark

See[ medium article for more details.](https://medium.com/@orellabac/ingest-external-data-into-snowflake-with-snowpark-and-jdbc-eb487b61078c)

**Building**

```
mvn clean package
```

The target file will be at:  `target/snowpark-jdbc-reader-0.0.1.jar`

For convience there is a prebuilt-jar you can use.

**Deployment**

upload the jar to snowflake, you can use the [SnowSight UI to do that easily ](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui)

and create an UDF like this:

```




CREATE OR REPLACE SECRET external_database_cred
    TYPE = password
    USERNAME = 'serveradmin'
    PASSWORD = 'xxxxxxxxx';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION external_database_network_rule_ext_int
  ALLOWED_NETWORK_RULES = (external_database_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (external_database_cred)
  ENABLED = true;


CREATE OR REPLACE FUNCTION READ_JDBC(OPTION OBJECT, query STRING)
  RETURNS TABLE(data OBJECT)
  LANGUAGE JAVA
RUNTIME_VERSION='11'
  IMPORTS = ('@mystage/snowpark-jdbc-reader-0.0.1.jar' ) -- Add the jar for any jdbc driver you need in this section
  EXTERNAL_ACCESS_INTEGRATIONS = (external_database_network_rule_ext_int)
  SECRETS = ('cred' = external_database_cred )
  HANDLER = 'JdbcDataReader';
```
You can see an example for python in snowflake [here](https://github.com/Snowflake-Labs/snowpark-extensions-py/blob/main/extras/jdbc_read/using_jdbc_reader_notebook.ipynb)

