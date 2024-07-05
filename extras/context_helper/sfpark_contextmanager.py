import logging

class SessionContextManager:
    def __init__(self, session):
        self.session = session
    
    def __enter__(self):
        self.session.connection.cursor().execute('BEGIN')
        print("BEGIN")
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print("ERROR==>",exc_val)
            logging.error(exc_val)
            self.session.connection.cursor().execute('ROLLBACK')
            print("ROLLBACK")
            return True
        else:
            self.session.connection.cursor().execute('COMMIT')
            print("COMMIT")