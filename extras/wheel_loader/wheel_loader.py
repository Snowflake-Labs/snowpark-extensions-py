class FileLock:
   def __enter__(self):
      self._lock = threading.Lock()
      self._lock.acquire()
      self._fd = open('/tmp/lockfile.LOCK', 'w+')
      fcntl.lockf(self._fd, fcntl.LOCK_EX)

   def __exit__(self, type, value, traceback):
      self._fd.close()
      self._lock.release()

import sys,os,threading,fcntl,zipfile
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

def load(file_name):
    path=import_dir + file_name
    extracted='/tmp/'+file_name
    with FileLock():
        if not os.path.isdir(extracted):
            with zipfile.ZipFile(path, 'r') as myzip:
                myzip.extractall(extracted)
    sys.path.append(extracted)