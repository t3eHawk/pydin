from .log import make_log, make_table
from .utils import who, when
from .dates import Today
from .config import make_config
from .nodes.database import make_database

class Task():
    def __init__(self, name=None, config=None, log=None, job=None, date=None):
        # Name of the pipeline. If name was not defined and pipeline is a part
        # of a job then take its name.
        if name is None and hasattr(job, 'name') is True: name = job.name

        # System logger. If logger was not passed and pipeline is a part of
        # a job then try to take its logger. If still no logger was defined
        # then use main logger.
        if log is None and hasattr(job, 'log') is True: log = job.log

        if date is None and hasattr(job, 'date') is True: date = job.date

        # The username who initiated the pipeline. If part of the job then
        # take its initiator that can be either username or scheduler.
        initiator = None
        if job is not None and hasattr(job, 'initiator') is True:
            initiator = job.initiator

        # Process configurator.
        self.config = make_config(obj=config)
        for key in ('meta', 'logging'):
            if self.config.get(key) is None:
                self.config[key] = {}

        # The name of the pipeline.
        self.name = self.config['meta'].get('name', name)
        # Job used to deploy this pipeline.
        self.job = job
        # Logger used for pipeline.
        self.log = make_log(obj=log)

        self.operation_id = None
        # Moment for which pipeline initiated.
        self.date = date or when()
        self.today = Today(self.date)
        # Time that reflects data period.
        self.operation_date = self.date
        # Who initiated the pipeline.
        self.initiator = initiator or who()
        # Time of pipeline execution
        self.start_date = None
        self.end_date = None
        # Count statistics of input and output objects.
        self.read = 0
        self.written = 0
        self.updated = 0
        self.errros = 0
        # Operation status.
        self.status = None

        # Process table logging configuration.
        if isinstance(self.config.get('logging'), dict) is True:
            if self.log.table.status is False:
                db = self.config['logging'].get('database')
                name = self.config['logging'].get('name')
                schema = self.config['logging'].get('schema')
                if db is not None and name is not None:
                    db = make_database(db)
                    table = make_table(db, name, schema=schema)
                    self.log.table.configure(db=db.connection, proxy=table,
                                             date_column='log_date')
                    self.log.table.open()
            elif self.log.table.status is True:
                self.log.table.new()
            job = job.id if hasattr(self.job, 'id') is True else None
            self.log.set(operation_date=self.operation_date,
                         initiator=self.initiator,
                         job=job)
            self.operation_id = self.log.table.primary_key

        pass

    def result(self):
        pass

    def run(self):
        self._prepare()
        self._execute()
        self._finalize()
        return self.result()

    def prepare(self):
        pass

    def execute(self):
        pass

    def finalize(self):
        pass

    def _prepare(self):
        self.log.bound()
        self.log.info(f'Opening pipeline <{self.name}>...')
        self.log.info(f'Operation ID <{self.operation_id}>')
        self.log.info(f'Operation date <{self.operation_date}>')
        try:
            self.prepare()
            self.start_date = when()
            self.log.info(f'Start at {self.start_date}')
            self.log.set(start_date=self.start_date)
        except:
            self.log.critical()
        else:
            self.log.info('Pipeline opened')
        pass

    def _execute(self):
        self.log.subhead('execution')
        try:
            self.status = 'P'
            self.log.set(status=self.status)
            self.execute()
        except:
            self.status = 'E'
            self.log.set(status=self.status)
            self.log.critical()
        else:
            self.status = 'D' if self.log.with_error is False else 'E'
            self.log.set(status=self.status)
            self.log.info('Execution finished')
        pass

    def _finalize(self):
        self.log.bound()
        self.log.info(f'Closing pipeline <{self.name}>...')
        try:
            self.finalize()
            self.end_date = when()
            self.log.info(f'End at {self.end_date}')
            self.log.set(end_date=when())
        except:
            self.log.critical()
        else:
            self.log.info('Pipeline closed')
        pass
