
import sys, json, base64, io
from snowflake.snowpark import Session
import logging

try:
    import streamlit as st
except:
    # in notebooks streamlit will always if not present, well we will just mock it
    # this will allow us to use the same code in notebooks and or for example 
    # functions or procedures
    class StreamlitMock:
        def __init__(self):
            self.session_state = {}
        def text_input(self, labelOrName,value=None,key=None):
            if value is None:
                value = self.session_state.get("args").get(labelOrName)
            return value
    st = StreamlitMock()

import importlib
import inspect

def run(module_name):
    """
    Loads all the module elements into the current scope
    """
    caller_globals = inspect.currentframe().f_back.f_globals
    module = importlib.import_module(module_name)
    caller_globals.update({k: v for k, v in module.__dict__.items() if not k.startswith('_')})


class WidgetUtils:
    def __init__(self):
        self.args = {}
        self.load_args(sys.argv)
    def load_args(self,args):
        # reset values
        self.args = {}
        if isinstance(args,str):
            args = args.split(' ')
        if len(args)==1 and args[0]:
            try:
                json_string = base64.b64decode(args[0]).decode('utf-8')
                self.args = json.loads(json_string)
                st.session_state["args"] = self.args
                return
            except Exception as ex:
                print("Failed to load args from base64 encoded string: {args[0]}", ex)
                # ignore and continue
        for i,arg in enumerate(args):
            if "=" in arg:
                try:
                    name, value = arg.split("=")
                    self.args[name] = value
                except:
                    logging.error(f"Failed to load args processing value pair: {arg}, loading as arg{i+1}", ex)
                    self.args[f"arg{i+1}"] = arg
            else:
                self.args[f"arg{i+1}"] = arg
        st.session_state["args"] = self.args
                
    def text(self,name: str, defaultValue: str=None, label: str=None):
        if defaultValue is None:
            defaultValue = st.session_state.get("args").get(name)
        new_value = st.text_input(label or name,value=defaultValue,key=name+"wig_text")
        st.session_state["args"][name] = new_value
        return new_value
    def get(self,name):
        return st.session_state.get("args").get(name)

class FileSystemUtils:
    def __init__(self):
        self.session = Session.builder.getOrCreate()
    def ls(self,path:str):
        files = self.session.sql(f"ls {path}").collect()
        return [r[0] for r in files]
    def rm(self,path:str):
        self.session.sql(f"rm {path}")
    def put(self,file:str,contents:str,overwrite:bool = False) -> bool:
        try:
            # Convert the string to bytes
            my_bytes = contents.encode('utf-8')
            # Create a BytesIO object from the bytes
            bytes_io = io.BytesIO(my_bytes)
            result = self.session.file.put_stream(bytes_io, file,auto_compress=False,overwrite=overwrite)
            print(f"Wrote {result.source_size} bytes to {file}")
            return True
        except:
            return False
    def mkdirs(self,path:str):
        print(f"operation ignored. for {path}")
        ...
    

class NotebookUtils:
    def __init__(self):
        self.session = Session.builder.getOrCreate()
    def exit(msg:str,permanent=False):
        logging.info(f"NOTEBOOK EXIT {msg}")
        # saving notebook info
        session_id = self.session._session_id
        encoded_msg = base64.b64encode(msg)
        if permanent:
            self.session.sql(f"CREATE TABLE IF NOT EXISTS __NOTEBOOK_EXITINFO__(SESSION_ID STRING, EXIT_MSG STRING)").show()
            self.session.sql(f"INSERT INTO __NOTEBOOK_EXITINFO__(SESSION_ID, EXIT_MSG) VALUES ('{session_id}', '{encoded_msg}')").show()
        else:
            self.session.sql("CREATE OR REPLACE TEMP TABLE __NOTEBOOK_EXIT AS SELECT '{msg}' AS EXIT_MSG")

widgets = WidgetUtils()
fs = FileSystemUtils()
notebook = NotebookUtils()