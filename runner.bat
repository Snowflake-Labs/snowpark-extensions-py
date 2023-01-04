@echo off
setlocal
REM Set the python io encoding to UTF-8 by default if not set.
IF "%PYTHONIOENCODING%"=="" (
    SET PYTHONIOENCODING="UTF-8"
)
python "%~dp0\runner" %*

endlocal