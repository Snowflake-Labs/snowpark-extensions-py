#!/usr/bin/python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

import argparse
from rich import print
import os
import json

from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType, VariantType
from snowflake.snowpark.functions import sproc
import logging
import configparser

print("[cyan]Snowpark Extensions Extras")
print("[cyan]Notebook Runner procedure")
print("[cyan]=============================")
arguments = argparse.ArgumentParser()
arguments.add_argument("--stage"   ,help="stage where the proc code will be uploaded",default="NOTEBOOK_RUN")
arguments.add_argument("--packages",help="packages that will be available for the notebook code",default="")
arguments.add_argument("--imports" ,help="imports that will be available for the notebook code" ,default="")
arguments.add_argument("--procedure" ,help="procedure name" ,default="")
arguments.add_argument("--connection",dest="connection_args",nargs="*",required=True,help="Connect options, for example snowsql, snowsql connection,env")


def snowsql_config(self,section=None,configpath=os.path.expanduser('~{os.sep}.snowsql{os.sep}config')):
    if not os.path.exists(configpath):
        logging.error(f"No snowsql config found at:{configpath}")
        return self
    config = configparser.ConfigParser()
    config.read(configpath)
    config_section_name = "connections" if section is None else f"connections.{section}"
    if config_section_name in config:
        section = config[config_section_name]
        if section:
            connection_parameters = {
                "user":section.get("username"),
                "password":section.get("password"),
                "account":section.get('accountname'),
                "role":section.get('rolename'),
                "warehouse":section.get('warehousename'),
                "database":section.get("dbname"),
                "schema":section.get("schemaname")
            }
            return connection_parameters
    else:
        logging.error(f"Config section {config_section_name} not found in snowsql file: {configpath}")
    return {}

def env(self):
    sf_user      = os.getenv("SNOW_USER")     or os.getenv("SNOWSQL_USER")
    sf_password  = os.getenv("SNOW_PASSWORD") or os.getenv("SNOWSQL_PWD")
    sf_account   = os.getenv("SNOW_ACCOUNT")  or os.getenv("SNOWSQL_ACCOUNT")
    sf_role      = os.getenv("SNOW_ROLE")     or os.getenv("SNOWSQL_ROLE")
    sf_warehouse = os.getenv("SNOW_WAREHOUSE") or os.getenv("SNOWSQL_WAREHOUSE")
    sf_database  = os.getenv("SNOW_DATABASE") or os.getenv("SNOWSQL_DATABASE")
    options = {}
    if sf_user is not None:
        options["user"]     = os.getenv("SNOW_USER") or os.getenv("SNOWSQL_USER")
    if sf_password is not None:
        options["password"] = sf_password
    if sf_account is not None:
        options["account"]  = sf_account
    if sf_role is not None:
        options["role"]     = sf_role
    if sf_warehouse is not None:
        options["warehouse"]= sf_warehouse
    if sf_database is not None:
        options["database"] = sf_database
    return options

args = arguments.parse_args()
session = None
try:
    if len(args.connection_args) >= 1:
        first_arg = args.connection_args[0]
        rest_args = args.connection_args[1:]
        if first_arg == "snowsql":
            session = Session.builder.configs(snowsql_config(*rest_args)).getOrCreate()
        elif first_arg == "env":
            session = Session.builder.configs(env())
        elif first_arg == "json":
            session = Session.builder.configs(json.load(open(rest_args[0]))).getOrCreate()
        else:
            connection_args={}
            for arg in args.connection_args:
                key, value = arg.split("=")
                connection_args[key] = value
            session = Session.builder.configs(connection_args).getOrCreate()
except Exception as e:
    print(e)
    print("[red] An error happened while trying to connect")
    exit(1)
if not session:
    print("[red] Not connected. Aborting")
    exit(2)
session.sql(f"CREATE STAGE IF NOT EXISTS {args.stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").show()

packages=["snowflake-snowpark-python","nbconvert","nbformat","ipython","jinja2==3.0.3","plotly"]
packages.extend(set(filter(None, args.packages.split(','))))
print(f"1. Using packages [magenta]{packages}")
session.add_packages(packages)
imports=[]
if args.imports:
    imports.extend(args.imports.split(','))
print(f"2. Using imports [magenta]{imports}")
for i in imports:
    session.add_import(i)

print(f"3. registering procedure")
curdir=os.path.dirname(__file__)
session.sproc.register_from_file(os.path.join(curdir,"runner.py"),
    func_name="main",
    name=args.procedure,
    replace=True,
    is_permanent=True,
    stage_location=args.stage,
    return_type=StringType(),
    input_types=[StringType(),StringType(),VariantType()])