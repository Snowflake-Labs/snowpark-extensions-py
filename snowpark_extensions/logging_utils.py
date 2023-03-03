def logged(procname):
    import logging, datetime
    from functools import wraps
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_filename = f"/tmp/{procname}_{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log"
            logging.basicConfig(filename=log_filename, level=logging.DEBUG)
            try:
                result = func(*args, **kwargs)
            except Exception as ex:
                logging.exception(ex)
                result = "error"
            finally:
                logging.shutdown()
                session = args[0]
                session.file.put(f'file://{log_filename}','@~/')
            return result
        return wrapper

# this code has been borrow from: 
# Simple Tags in Snowflake Snowpark for Python
# https://medium.com/snowflake/simple-tags-in-snowflake-snowpark-for-python-c5910749273
# Special thanks to Bart

from contextlib import ContextDecorator
class Tag(ContextDecorator):
    def __init__(self, session, tag_name, label=None):
        self.old_tag = None
        self.session = session
        self.tag_name = tag_name
        self.label = None
    def __call__(self, func):
        if self.label is None:  # Label was not provided
            self.label = func.__name__  # Use function's name.
        return super().__call__(func)
    def __enter__(self):
        self.old_tag = self.session.query_tag
        if self.label:
            self.session.query_tag=f"{self.tag_name}_{self.label}"
        else:
            self.session.query_tag=self.tag_name
        return self
    def __exit__(self, *exc):
        self.session.query_tag=self.old_tag
        return False