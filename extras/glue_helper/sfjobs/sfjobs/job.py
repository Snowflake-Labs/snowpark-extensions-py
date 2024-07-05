import logging
class Job:
    def __init__(self, context):
        self._context = context
    def init(self, job_name, args={}):
        self._job_name = job_name
        self._args = args
        
    def commit(self):
        logging.info('Committing job')