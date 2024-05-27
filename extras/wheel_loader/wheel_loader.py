import sys, os
import threading
import fcntl
import zipfile
from pathlib import Path
import logging
from functools import lru_cache
from snowflake.snowpark._internal.utils import is_in_stored_procedure

class FileLock:
   def __enter__(self):
        self._lock = threading.Lock()
        self._lock.acquire()
        self._fd = open('/tmp/wheel_loader.LOCK', 'w+')
        fcntl.lockf(self._fd, fcntl.LOCK_EX)

   def __exit__(self, type, value, traceback):
        self._fd.close()
        self._lock.release()

@lru_cache()
def load(whl_name,append=True, use_lock=True):
    logging.info(f"loading wheel {whl_name}")
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
    return message

# this decoration will make sure that this does not get loaded more that one
@lru_cache(maxsize=1)
def add_wheels():
    if not is_in_stored_procedure():
        message = "Wheel loader can only be used in stored procedures"
        logging.warning(message)
        return message
    if os.getenv('HOME') is None:
        # many wheels need a home directory
        logging.info("HOME dir was not set. Setting up one now")
        os.environ["HOME"] = "/tmp/homedir"
        os.makedirs(os.environ["HOME"], exist_ok=True)
    wheels = [x for x in os.listdir(sys._xoptions['snowflake_import_directory']) if x.endswith('.whl')]
    with FileLock():
        for whl in wheels:
            load(whl, use_lock=False) # we use one lock for all
    message = str(wheels) + " where loaded"
    logging.info(message)
    logging.info("sys path is now " + str(sys.path))
    return message
