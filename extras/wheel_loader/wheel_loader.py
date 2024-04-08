import sys
import threading
import fcntl
import zipfile
from pathlib import Path


class FileLock:
   def __enter__(self):
        self._lock = threading.Lock()
        self._lock.acquire()
        self._fd = open('/tmp/wheel_loader.LOCK', 'w+')
        fcntl.lockf(self._fd, fcntl.LOCK_EX)

   def __exit__(self, type, value, traceback):
        self._fd.close()
        self._lock.release()


def load(whl_name):
    whl_path = Path(sys._xoptions['snowflake_import_directory']) / whl_name
    extraction_path = Path('/tmp') / whl_name

    with FileLock():
        if not extraction_path.is_dir():
            with zipfile.ZipFile(whl_path, 'r') as h_zip:
                h_zip.extractall(extraction_path)

    sys.path.append(str(extraction_path))
