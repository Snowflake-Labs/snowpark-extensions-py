# Notebook Runner

The notebook runner is a small example that allows you to run a notebook from within snowflake.

The runner script will:
1. connect to snowflake 
2. upload the notebook, 
3. publish a storeproc, 
4. run the store procedure, 
5. save the results of the notebook and 
6. then download the results as an html

This script call also be used to publish a permanent stored proc that can then be used to run any notebook that is already on an stage,
or to schedule a task to run a notebook.

