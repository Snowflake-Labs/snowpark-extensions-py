import sys
import types
import tempfile
from importlib.abc import MetaPathFinder, Loader
from importlib.util import spec_from_loader
from snowflake.snowpark import Session
import inspect
import os
import types

current_stage_for_import = None

def set_current_stage_for_import(stage_name):
    global current_stage_for_import
    current_stage_for_import = stage_name

# Step 1: Subclass types.ModuleType to create a custom module type
class SnowflakeStageModule(types.ModuleType):
    def __init__(self, name, stage_name):
        super().__init__(name)
        self.stage_name = stage_name
        self.__dict__["__path__"] = stage_name

class SnowflakeStageModuleFile(types.ModuleType):
    def __init__(self, name, stage_name,globals,origin):
        super().__init__(name)
        self.name = name
        self.stage_name = stage_name
        self.globals    = globals
        self.origin     = origin
        self.load()
    def load(self):
        self.loaded = True
        temp_dir = self.origin[0]
        module_filename = self.origin[1]
        current_dir = os.getcwd()  # Get the current working directory
        try:
            os.chdir(temp_dir)  # when loading this file we need to be in the directory where the file is
            exec(open(os.path.join(temp_dir, module_filename + ".py")).read(), self.globals)
        except FileNotFoundError as fNotFoundError:
            if fNotFoundError.filename.startswith(temp_dir):
                filename = fNotFoundError.filename.replace(temp_dir, "")
                new_ex = FileNotFoundError(f"File {filename + '.py'} not found")
                new_ex.filename = filename
                raise new_ex
            else:
                raise
        finally:
            os.chdir(current_dir)  # Change back to it

def find_main_globals():
    # Traverse the stack in the current thread
    for frame_info in inspect.stack():
        frame = frame_info.frame  # Get the frame object
        if frame.f_globals.get("__name__") == "__main__":
            # If the frame is the main module, return its globals
            return frame.f_globals
    return None  # Return None if no main module frame is found


STAGE_PREFIX="__STAGE_IMPORT__"
class SnowflakeStageFinder(MetaPathFinder):
    def __init__(self):
        self.stages = {}
        self.tempdirs_for_stage_files = {}
    def load_stage_files(self,stage_name):
        # Connect to Snowflake and load the file corresponding to the module
        session = Session.builder.getOrCreate()
        stage_files = [x[0] for x in session.sql(f"ls @{stage_name}").collect()]
        tempdir_ref = tempfile.mkdtemp()
        for stage_file in stage_files:
            session.file.get(f"@{stage_file}", tempdir_ref)
            print(f"Downloaded {stage_file} to {tempdir_ref}")
        self.tempdirs_for_stage_files[stage_name] = tempdir_ref
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith(STAGE_PREFIX) and fullname.endswith("__") and not "." in fullname:
            caller_globals = find_main_globals()
            stage_name = fullname.replace(STAGE_PREFIX,"")[:-2]
            if stage_name == "":
                stage_name = current_stage_for_import
            if stage_name is None or stage_name.strip() == "":
                raise Exception("No stage name specified")
            file_loader = SnowflakeFileLoader(stage_name,caller_globals)
            self.load_stage_files(stage_name)
            return spec_from_loader(fullname, file_loader)
        elif fullname.startswith(STAGE_PREFIX) and "." in fullname:
            caller_globals = find_main_globals()
            stage_name = fullname.split(".")[0].replace(STAGE_PREFIX,"")[:-2]
            filename = fullname.split(".")[-1]
            temp_dir = self.tempdirs_for_stage_files[stage_name]
            return spec_from_loader(fullname, SnowflakeFileLoader(stage_name,caller_globals, temp_dir),origin=(temp_dir,filename))
        return None

class SnowflakeFileLoader(Loader):
    def __init__(self, stage_name, globals,loader=None):
        self.stage_name = stage_name
        self.globals = globals
        self.loader = loader
    def create_module(self, spec):
        if spec.origin is None:
            return SnowflakeStageModule(spec.name, self.stage_name)
        else:
            return SnowflakeStageModuleFile(spec.name, self.stage_name, self.globals, spec.origin,)
    def exec_module(self, module):
       ...

sys.meta_path.insert(0, SnowflakeStageFinder())
