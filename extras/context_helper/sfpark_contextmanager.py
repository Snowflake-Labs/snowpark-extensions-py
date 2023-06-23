import logging

class SessionContextManager:
    def __init__(self, builder_object):
        self.builder_object = builder_object
        self.session = None
    
    def __enter__(self):
        self.session = self.builder_object.create()
        self.session.sql('BEGIN').collect()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print("ERROR==>",exc_val)
            logging.error(exc_val)
            self.session.sql('ROLLBACK').collect()
            return True
        else:
            self.session.sql('COMMIT').collect()
        self.session.close()