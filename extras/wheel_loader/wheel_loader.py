import sys, os
import threading
import fcntl
import zipfile
from pathlib import Path
import logging
from functools import lru_cache
import tarfile

class FileLock:
   def __enter__(self):
        self._lock = threading.Lock()
        self._lock.acquire()
        self._fd = open('/tmp/wheel_loader.LOCK', 'w+')
        fcntl.lockf(self._fd, fcntl.LOCK_EX)

   def __exit__(self, type, value, traceback):
        self._fd.close()
        self._lock.release()

@lru_cache
def load_tgz(tgz_name,append=True, use_lock=True):
    logging.info(f"loading tgz {tgz_name}")
    tgz_path = Path(sys._xoptions['snowflake_import_directory']) / tgz_name
    extraction_path = Path('/tmp') / tgz_name
    if use_lock:
        with FileLock():
            if not extraction_path.is_dir():
                with tarfile.open(tgz_path, 'r:gz') as tar:
                    tar.extractall(extraction_path)
    else:
        if not extraction_path.is_dir():
                with tarfile.open(tgz_path, 'r:gz') as tar:
                    tar.extractall(extraction_path)
    ext = ".tgz" if tgz_name.endswith(".tgz") else ".tar.gz"
    inner_folder = tgz_name.replace(ext,"")
    if append:
        sys.path.append(str(extraction_path / inner_folder))
        message = f"tgz {tgz_name} added to the end of path"
        logging.info(message)
        return message
    else:
        sys.path.insert(0,str(extraction_path / inner_folder))
        message = f"tgz {tgz_name} added to the front of path"
        logging.info(message)
    try:
        # to enable pck_resource resolution
        import pkg_resources
        # Add a directory to the pkg_resources working set
        pkg_resources.working_set.add_entry(str(extraction_path / inner_folder))
    except Exception as ex:
        logging.error(f"failed to add {extraction_path / inner_folder} to pkg_resources working set: {ex}")
    return message

@lru_cache()
def load(whl_name,append=True, use_lock=True):
    logging.info(f"loading wheel {whl_name}")
    if whl_name.startswith("@"):
        # this is only expected to be used in notebooks
        import snowflake.snowpark
        session = snowflake.snowpark.Session.builder.getOrCreate()
        os.makedirs("/tmp/whl_download/",exist_ok=True)
        session.file.get(whl_name,"/tmp/whl_download/")
        whl_path = Path("/tmp/whl_download/") / os.path.basename(whl_name)
        extraction_path = Path('/tmp') / os.path.basename(whl_name)
    else:
        whl_path = Path(sys._xoptions['snowflake_import_directory']) / whl_name
        extraction_path = Path('/tmp') / whl_name

    if use_lock:
        with FileLock():
            if not extraction_path.is_dir():
                with zipfile.ZipFile(whl_path, 'r') as h_zip:
                    h_zip.extractall(extraction_path)
    else:
        if not extraction_path.is_dir():
                with zipfile.ZipFile(whl_path, 'r') as h_zip:
                    h_zip.extractall(extraction_path)
    if append:
        sys.path.append(str(extraction_path))
        message = f"wheel {whl_name} added to the end of path"
        logging.info(message)
        return message
    else:
        sys.path.insert(0,str(extraction_path))
        message = f"wheel {whl_name} added to the front of path"
        logging.info(message)
    try:
        # to enable pck_resource resolution
        import pkg_resources
        # Add a directory to the pkg_resources working set
        pkg_resources.working_set.add_entry(str(extraction_path))
    except Exception as ex:
        logging.warning(f"failed to add {extraction_path} to pkg_resources working set: {ex}")
    return message

def setup_home():
    if os.getenv('HOME') is None or os.getenv('HOME') == '/home/udf':
        # many wheels need a home directory
        logging.info("HOME dir was not set. Setting up one now")
        os.environ["HOME"] = "/tmp/homedir"
        os.makedirs(os.environ["HOME"], exist_ok=True)


# this decoration will make sure that this does not get loaded more that one
@lru_cache(maxsize=1)
def add_wheels(from_stage=None):
    try:
        import snowflake.snowpark._internal.utils
        if not snowflake.snowpark._internal.utils.is_in_stored_procedure():
            message = "Wheel loader can only be used in stored procedures"
            logging.warning(message)
            return message
    except:
        pass
    setup_home()
    if from_stage and from_stage.startswith("@"):
        try:
            import snowflake.snowpark
            session = snowflake.snowpark.Session.builder.getOrCreate()
            wheels = [("@" + r[0]) for r in session.sql(f"ls {from_stage} pattern='.*whl'").collect()]
        except Exception as exceptionStageFiles:
            message = "Error while getting wheels from stage: " + str(exceptionStageFiles)
            logging.warning(message)
            return message
    else:
        wheels = [x for x in os.listdir(sys._xoptions['snowflake_import_directory']) if x.endswith('.whl')]
    with FileLock():
        for whl in wheels:
            load(whl, False) # we use one lock for all
    message = str(wheels) + " wheels where loaded"
    logging.info(message)
    return message

# this decoration will make sure that this does not get loaded more that one
@lru_cache(maxsize=1)
def add_tars():
    try:
        import snowflake.snowpark._internal.utils
        if not snowflake.snowpark._internal.utils.is_in_stored_procedure():
            message = "tgz loader can only be used in stored procedures"
            logging.warning(message)
            return message
    except:
        pass
    setup_home()
    tars = [x for x in os.listdir(sys._xoptions['snowflake_import_directory']) if x.endswith('.tgz') or x.endswith('tar.gz')]
    with FileLock():
        for whl in tars:
            load_tgz(whl, False) # we use one lock for all
    message = str(tars) + " where loaded"
    logging.info(message)
    return message