
-- you can have one network rule were you add all the servers you want to connect to
-- or you can have multiple network rules for different servers
CREATE OR REPLACE NETWORK RULE jdbc_read_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
'mydatabase.database.windows.net:1433',
'production.database.cloud:1433',
'pgdb.example.com:5432',
'mysqlserver1.example.com:3306');

-- if you want to use private link then you need
-- a rule for PRIVATE_HOST_POST
CREATE OR REPLACE NETWORK RULE jdbc_read_network_rule_private
  MODE = EGRESS
  TYPE = PRIVATE_HOST_PORT
  VALUE_LIST = ('mydatabase.database.windows.net:1433');

-- create a secret for the password FOR each database
-- normally I recommend to have a common prefix (for example 'jdbc_secret_conf_' ) for the secrets
-- when you configure your secrets in the UDF normally i setup the alias 
-- removing the prefix
CREATE SECRET jdbc_secret_conf_sqlserver1
  TYPE = password
  USERNAME = 'serveradmin'
  PASSWORD = 'xxxxxxx';



CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION jdbc_read_access_integration
  ALLOWED_NETWORK_RULES = (
   jdbc_read_network_rule
  )
  ALLOWED_AUTHENTICATION_SECRETS = (
    -- add any secrets here, we will discuss about that next
    jdbc_secret_conf_sqlserver1
  )
  ENABLED = true;

-- upload the jar for the UDF
put file://extras/jdbc_read_into_parquet/target/jdbcreader-1.0-SNAPSHOT-jar-with-dependencies.jar 
@mystage auto_compress=False overwrite=True;

CREATE OR REPLACE PROCEDURE READ_JDBC2(JDBC_OPTIONS OBJECT, query STRING )
RETURNS VARCHAR
LANGUAGE JAVA
RUNTIME_VERSION = 11
PACKAGES = ('com.snowflake:snowpark:latest')
IMPORTS = ('@mystage/jdbcreader-1.0-SNAPSHOT-jar-with-dependencies.jar')
HANDLER = 'JdbcDataReader.proc_main'
EXTERNAL_ACCESS_INTEGRATIONS = (jdbc_read_network_rule)
SECRETS = ('sqlserver1' = jdbc_secret_conf_sqlserver1)
EXECUTE AS CALLER;
