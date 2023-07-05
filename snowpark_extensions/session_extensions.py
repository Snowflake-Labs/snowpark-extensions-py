from snowflake.snowpark import Session
import os
import io
import sys

if not hasattr(Session,"___extended"):
    setattr(Session,"___extended",True)
    def __repr__(self):
        from snowflake.snowpark import Session
        from snowflake.snowpark.version import VERSION
        snowflake_environment = self.sql('select current_version()').collect()
        snowpark_version = VERSION
        # Current Environment Details
        return f'Role                        : {self.get_current_role()}\n' +\
        f'Warehouse                   : {self.get_current_warehouse()}\n' +\
        f'Database                    : {self.get_current_database()}\n' +\
        f'Schema                      : {self.get_current_schema()}\n' +\
        f'Snowflake version           : {snowflake_environment[0][0]}\n' +\
        f'Snowpark for Python version : {snowpark_version[0]}.{snowpark_version[1]}.{snowpark_version[2]}\n'
    
    # this method is added for IPython compatibility
    setattr(Session,"__repr__",__repr__)