"""Contains main application configurator and other configuration elements."""

import calendar as cnd
import datetime as dt
import importlib as il
import ftplib
import configparser
import os
import platform
import sys

import paramiko as po
import pepperoni as pe
import sqlalchemy as sa


home = os.environ.get('DEVOE_HOME')
filename = 'devoe.ini'
user_config = os.path.abspath(os.path.expanduser(f'~/.devoe/{filename}'))
home_config = os.path.join(home, filename) if home is not None else None
local_config = os.path.join(os.path.dirname(sys.argv[0]), filename)
index_config = os.path.abspath(os.path.expanduser(f'~/.devoe/index.ini'))


class Configurator(dict):
    """Represents main application configurator."""

    def __init__(self):
        super().__init__()
        tables = ['GENERAL', 'DATABASE', 'SMTP', 'API',
                  'ENVIRONMENTS', 'SCHEDULER', 'JOB',
                  'LOGGING', 'EMAIL']
        for table in tables:
            self[table] = self.Table(name=table)
        pass

    class Table(dict):
        """Represents configuration table."""

        def __init__(self, name, **kwargs):
            self.name = name
            super().__init__(**kwargs)
            pass

        def __setitem__(self, key, value):
            """Normalize given item key and set item value."""
            super().__setitem__(key.lower(), value)
            pass

        def __getitem__(self, key):
            """Normalize given item key and get item value if it exists."""
            return super().__getitem__(key.lower())

        def get(self, key):
            """Normalize given item key and get item value or return None."""
            return super().get(key.lower())

        def has_record(self, record):
            """Check if configuration table record exists."""
            if self.get(record) is not None:
                return True
            else:
                return False
            pass

        pass

    def load(self, paths, encoding=None):
        """Load extetrnal configuration from files to configurator."""
        paths = [path for path in paths if path is not None]
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(paths, encoding=encoding)
        for section in parser.sections():
            for option in parser.options(section):
                value = parser[section][option]
                if (
                    value is None
                    or value.upper() == 'NONE'
                    or value.isspace() is True
                ):
                    self[section][option] = None
                elif value.upper() == 'TRUE':
                    self[section][option] = True
                elif value.upper() == 'FALSE':
                    self[section][option] = False
                elif value.isdigit() is True:
                    self[section][option] = int(value)
                elif value.isdecimal() is True:
                    self[section][option] = float(value)
                else:
                    self[section][option] = value
        return self

    def has_table(self, table):
        """Check if configuration table exists."""
        if self.get(table) is not None:
            return True
        else:
            return False
        pass


class Logging():
    """Represents logging configurator used for special loggers."""

    def __init__(self, task=True, step=False, database=None,
                 schema=None, table=None,  **fields):
        self.metadata = sa.MetaData()

        if task is False:
            self.task = self.Task(self, table=None)
        elif isinstance(task, dict):
            self.task = self.Task(self, table=task.get('table', table),
                                  schema=task.get('schema', schema),
                                  database=task.get('database', database),
                                  **task.get('fields', fields))
        else:
            table = table if not isinstance(task, str) else task
            self.task = self.Task(self, table=table, schema=schema,
                                  database=database, **fields)

        if step is False:
            self.step = self.Step(self, table=True)
        elif isinstance(step, dict):
            self.step = self.Step(self, table=step.get('table', step),
                                  schema=step.get('schema', schema),
                                  database=step.get('database', database),
                                  **step.get('fields', {}))
        else:
            table = None if not isinstance(step, str) else step
            self.step = self.Step(self, table=table, schema=schema,
                                  database=database)
        pass

    class Task():
        """Represents a generator for the task logger."""

        def __init__(self, logging, database=None,
                     schema=None, table=None, **fields):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('devoe.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
            self.fields = {'records_read': True,
                           'records_written': True,
                           'records_updated': True,
                           'records_merged': True,
                           'files_read': True,
                           'files_written': True,
                           'bytes_read': True,
                           'bytes_written': True,
                           'errors_found': True,
                           'initiator': False,
                           'server': False,
                           'user': False,
                           'file_log': False,
                           'text_log': False,
                           'text_error': False}
            self.optional = [key for key, value in self.fields.items()
                             if value is False]
            self.modify(**fields)
            pass

        def modify(self, **fields):
            """Modify table structure."""
            for key, value in fields.items():
                if key in self.fields.keys():
                    if isinstance(value, bool):
                        self.fields[key] = value
            pass

        def create_table(self):
            """Create database table used for logging."""
            name = self.table
            schema = self.schema
            metadata = self.logging.metadata
            columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                                 primary_key=True,
                                 comment='Unique task ID'),
                       sa.Column('job_id', sa.Integer,
                                 comment='Job that performed this task'),
                       sa.Column('run_id', sa.Integer,
                                 comment='Run that performed this task'),
                       sa.Column('run_date', sa.DateTime,
                                 comment='Date for which task was performed'),
                       sa.Column('task_name', sa.String(30),
                                 comment='Name of the task'),
                       sa.Column('updated', sa.DateTime,
                                 comment='Date of this record change'),
                       sa.Column('start_date', sa.DateTime,
                                 comment='Date when task started'),
                       sa.Column('end_date', sa.DateTime,
                                 comment='Date when task ended'),
                       sa.Column('status', sa.String(1),
                                 comment='Current status.'),
                       sa.Column('records_read', sa.Integer,
                                 comment='Total number of read records'),
                       sa.Column('records_written', sa.Integer,
                                 comment='Total number of written records'),
                       sa.Column('records_updated', sa.Integer,
                                 comment='Total number of updated records'),
                       sa.Column('files_read', sa.Integer,
                                 comment='Total number of read files'),
                       sa.Column('files_written', sa.Integer,
                                 comment='Total number of written files'),
                       sa.Column('bytes_read', sa.Integer,
                                 comment='Total volume of read bytes'),
                       sa.Column('bytes_written', sa.Integer,
                                 comment='Total volume of written bytes'),
                       sa.Column('errors_found', sa.Integer,
                                 comment='Total number of occurred errors'),
                       sa.Column('initiator', sa.String(1),
                                 comment='Type of initiation in the task'),
                       sa.Column('server', sa.String(30),
                                 comment='Host address with process'),
                       sa.Column('user', sa.String(30),
                                 comment='OS user who launched the process'),
                       sa.Column('file_log', sa.String(30),
                                 comment='Path to the log file'),
                       sa.Column('text_log', sa.Text,
                                 comment='Textual log records'),
                       sa.Column('text_error', sa.Text,
                                 comment='Textual error')]
            columns = [column for column in columns
                       if self.fields.get(column.name, True) is True]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self):
            """Configure database logger."""
            logger = pe.logger('devoe.task.logger', table=True,
                               console=False, file=False, email=False)
            if self.table is None:
                logger.configure(table=False)
            else:
                if not self.database.engine.has_table(self.table):
                    self.create_table()
                date_column = 'updated'
                logger.configure(db={'database': self.database,
                                     'name': self.table,
                                     'date_column': date_column})
            return logger

        pass

    class Step():
        """Represents a generator for the step logger."""

        def __init__(self, logging, database=None,
                     schema=None, table=None, **fields):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('devoe.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
            self.fields = {'records_read': True,
                           'records_written': True,
                           'records_updated': True,
                           'records_merged': True,
                           'files_read': True,
                           'files_written': True,
                           'bytes_read': True,
                           'bytes_written': True,
                           'errors_found': True,
                           'initiator': False,
                           'server': False,
                           'user': False,
                           'file_log': False,
                           'text_log': False,
                           'text_error': False}
            self.optional = [key for key, value in self.fields.items()
                             if value is False]
            self.modify(**fields)
            pass

        def modify(self, **fields):
            """Modify table structure."""
            for key, value in fields.items():
                if key in self.fields.keys():
                    if isinstance(value, bool):
                        self.fields[key] = value
            pass

        def create_table(self):
            """Create database table used for logging."""
            name = self.table
            schema = self.schema
            metadata = self.logging.metadata
            columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                                 primary_key=True,
                                 comment='Unique step ID'),
                       sa.Column('task_id', sa.Integer,
                                 comment='Task ID of this step'),
                       sa.Column('job_id', sa.Integer,
                                 comment='Job that performed this task'),
                       sa.Column('run_id', sa.Integer,
                                 comment='Run that performed this task'),
                       sa.Column('run_date', sa.DateTime,
                                 comment='Date for which task was performed'),
                       sa.Column('step_name', sa.String(30),
                                 comment='Name of the step'),
                       sa.Column('step_type', sa.String(3),
                                 comment='Type of the step'),
                       sa.Column('step_a', sa.String(30),
                                 comment='Name of the step item A'),
                       sa.Column('step_b', sa.String(30),
                                 comment='Name of the step item b'),
                       sa.Column('step_c', sa.String(30),
                                 comment='Name of the step item c'),
                       sa.Column('updated', sa.DateTime,
                                 comment='Date of this record change'),
                       sa.Column('start_date', sa.DateTime,
                                 comment='Date when task started'),
                       sa.Column('end_date', sa.DateTime,
                                 comment='Date when task ended'),
                       sa.Column('status', sa.String(1),
                                 comment='Current status.'),
                       sa.Column('records_read', sa.Integer,
                                 comment='Total number of read records'),
                       sa.Column('records_written', sa.Integer,
                                 comment='Total number of written records'),
                       sa.Column('records_updated', sa.Integer,
                                 comment='Total number of updated records'),
                       sa.Column('files_read', sa.Integer,
                                 comment='Total number of read files'),
                       sa.Column('files_written', sa.Integer,
                                 comment='Total number of written files'),
                       sa.Column('bytes_read', sa.Integer,
                                 comment='Total volume of read bytes'),
                       sa.Column('bytes_written', sa.Integer,
                                 comment='Total volume of written bytes'),
                       sa.Column('errors_found', sa.Integer,
                                 comment='Total number of occurred errors'),
                       sa.Column('initiator', sa.String(1),
                                 comment='Type of initiation in the task'),
                       sa.Column('server', sa.String(30),
                                 comment='Host address with process'),
                       sa.Column('user', sa.String(30),
                                 comment='OS user who launched the process'),
                       sa.Column('file_log', sa.String(30),
                                 comment='Path to the log file'),
                       sa.Column('text_log', sa.Text,
                                 comment='Textual log records'),
                       sa.Column('text_error', sa.Text,
                                 comment='Textual error')]
            columns = [column for column in columns
                       if self.fields.get(column.name, True) is True]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self, step):
            """Configure database logger."""
            logger = pe.logger(f'devoe.step.{step.seqno}.logger', table=True,
                               console=False, file=False, email=False)
            if self.table is None:
                logger.configure(table=False)
            else:
                if not self.database.engine.has_table(self.table):
                    self.create_table()
                date_column = 'updated'
                logger.configure(db={'database': self.database,
                                     'name': self.table,
                                     'date_column': date_column})
            return logger

        pass

    pass


class Localhost():
    """Represents localhost i.e. no connection required."""

    def __init__(self):
        self.host = platform.node()
        pass

    pass


class Server():
    """Represents ordinary remote server."""

    def __init__(self, name=None, host=None, port=None, user=None,
                 password=None, key=None, keyfile=None,
                 ssh=False, sftp=False, ftp=False):
        self.name = name.lower() if isinstance(name, str) else None
        self.host = host if isinstance(host, str) else None
        self.port = port if isinstance(port, (str, int)) else None

        self.creds = creds = {}
        creds['user'] = user if isinstance(user, str) else None
        creds['password'] = password if isinstance(password, str) else None
        creds['key'] = key if isinstance(key, str) else None
        creds['keyfile'] = keyfile if isinstance(keyfile, str) else None

        self.ssh = ssh if isinstance(ssh, bool) else False
        self.sftp = sftp if isinstance(sftp, bool) else False
        self.ftp = ftp if isinstance(ftp, bool) else False

        cache = il.import_module('devoe.cache')
        cache.CONNECTIONS[f'{self.name}'] = self
        pass

    def __repr__(self):
        """Represent server as its name."""
        return self.name

    class Connection():
        """Represents ordinary remote server connection."""

        def __init__(self, server, creds):
            server = server if isinstance(server, Server) else None
            user = creds.get('user')
            password = creds.get('password')
            keyfile = creds.get('keyfile')
            if server.ssh is True and server is not None:
                self.ssh = po.SSHClient()
                self.ssh.set_missing_host_key_policy(po.AutoAddPolicy())
                self.ssh.connect(server.host, port=server.port,
                                 username=user, password=password,
                                 key_filename=keyfile)
            if server.sftp is True and server is not None:
                if server.ssh is True:
                    self.sftp = self.ssh.open_sftp()
                else:
                    transport = po.Transport((server.host, server.port))
                    if keyfile is not None:
                        key = po.RSAKey(filename=keyfile)
                        transport.connect(username=user, pkey=key)
                    else:
                        transport.connect(username=user,
                                          password=password)
                    self.sftp = po.SFTPClient.from_transport(transport)
            if server.ftp is True and server is not None:
                self.ftp = ftplib.FTP(host=server.host,
                                      user=user,
                                      passwd=password)
            pass

        def execute(self, command, **kwargs):
            """Execute given command in server terminal."""
            stdin, stdout, stderr = self.ssh.exec_command(command, **kwargs)
            stdout = stdout.read().decode()
            stderr = stderr.read().decode()
            return stdout, stderr

        pass

    @property
    def connection(self):
        """Get active server connection."""
        return self._connection

    @connection.setter
    def connection(self, value):
        if isinstance(value, self.Connection):
            self._connection = value
        pass

    def connect(self):
        """Connect to remote server."""
        if self.connection is None:
            self.connection = self.Connection(self, self.creds)
        return self.connection

    def exists(self, path):
        """Check if file with given path exists in file system."""
        conn = self.connect()
        if self.sftp is True:
            try:
                conn.sftp.chdir(path)
                return True
            except IOError:
                return False
        elif self.ftp is True:
            try:
                conn.ftp.cwd(path)
                return True
            except Exception:
                return False

    pass


class Database(pe.Database):
    """Represents database server."""

    def __init__(self, name=None, vendor=None, driver=None, path=None,
                 host=None, port=None, sid=None, service=None,
                 user=None, password=None):
        self.name = name.lower() if isinstance(name, str) else None
        super().__init__(vendor=vendor, driver=driver,
                         path=path, host=host, port=port,
                         sid=sid, service=service,
                         user=user, password=password)

        cache = il.import_module('devoe.cache')
        cache.CONNECTIONS[f'{self.name}'] = self
        pass

    def __repr__(self):
        """Represent database as its name."""
        return self.name

    pass


class Connector(dict):
    """Represents connections configurator."""

    def __init__(self):
        super().__init__()
        pass

    class Connection():
        """Represents connection objects factory."""

        def __new__(self, name=None, **options):
            """Create connection object using options passed."""
            if options.get('database') is True:
                return Database(name=name, vendor=options['vendor'],
                                driver=options.get('driver'),
                                path=options.get('path'),
                                host=options.get('host'),
                                port=options.get('port'),
                                sid=options.get('sid'),
                                service=options.get('service'),
                                user=options.get('user'),
                                password=options.get('password'))
            elif (
                options.get('ssh') is True
                or options.get('sftp') is True
                or options.get('ftp') is True
            ):
                return Server(name=name, host=options.get('host'),
                              port=options.get('port'),
                              user=options.get('user'),
                              password=options.get('password'),
                              key=options.get('key'),
                              keyfile=options.get('keyfile'),
                              ssh=options.get('ssh'),
                              sftp=options.get('sftp'),
                              ftp=options.get('ftp'))
            else:
                return None
            pass

    def load(self, path=None, encoding=None):
        """Load configuration from file."""
        path = path or index_config
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(path, encoding=encoding)
        for section in parser.sections():
            section = section.lower()
            self[section] = {}
            for option in parser.options(section):
                option = option.lower()
                value = parser[section][option]
                if option in ('database', 'ssh', 'sftp', 'ftp'):
                    if value.upper() in ('TRUE', '1'):
                        self[section][option] = True
                    elif value.upper() in ('FALSE', '0'):
                        self[section][option] = False
                    else:
                        continue
                elif option in ('host', 'port', 'user',
                                'password', 'key', 'keyfile',
                                'vendor', 'driver',
                                'path', 'sid', 'service'):
                    self[section][option] = value
        return self

    def receive(self, name):
        """Receive connection object using its name."""
        if isinstance(name, str):
            name = name.lower()
            try:
                cache = il.import_module('devoe.cache')
                return cache.CONNECTIONS[name]
            except KeyError:
                try:
                    options = self[name]
                except KeyError as error:
                    path = index_config
                    message = f'no record named <{name}> found, check {path}'
                    traceback = error.__traceback__
                    raise Exception(message).with_traceback(traceback)
                else:
                    return self.Connection(name=name, **options)
        else:
            message = f'name must be str, not {name.__class__.__name__}'
            raise TypeError(message)
        pass

    pass


class Calendar():
    """Represents calendar configurator."""

    class Date():
        """Represents a single date."""

        def __init__(self, now=None):
            self.now = now
            self.timezone = None
            pass

        def __repr__(self):
            """Represent object as a datetime string."""
            return str(self.now)

        class value():
            """Represents wrapper used to calculate final date value."""

            def __init__(self, func):
                self.func = func
                self.__doc__ = func.__doc__
                pass

            def __call__(self, obj):
                """Get final date value."""
                datetime = self.func(obj)
                if obj.timezone is not None:
                    return datetime.astimezone(tz=obj.timezone)
                else:
                    return datetime
                pass

            pass

        @property
        @value
        def now(self):
            """Get current date."""
            return self._now

        @now.setter
        def now(self, value):
            if isinstance(value, dt.datetime) or value is None:
                self._now = value.replace(microsecond=0)
            pass

        @property
        @value
        def start(self):
            """Get start of current date."""
            return self._start

        @property
        @value
        def end(self):
            """Get end of current date."""
            return self._end

        @property
        def prev(self):
            """Get previous day."""
            now = self.now-dt.timedelta(days=1)
            return Calendar.Date(now)

        @property
        def next(self):
            """Get next day."""
            now = self.now+dt.timedelta(days=1)
            return Calendar.Date(now)

        @property
        def hour(self):
            """Get current hour."""
            return Calendar.Hour(self.now)

        @property
        def yesterday(self):
            """Get yesterday."""
            return Calendar.Yesterday(self.now)

        @property
        def tomorrow(self):
            """Get tomorrow."""
            return Calendar.Tomorrow(self.now)

        @property
        def month(self):
            """Get current month."""
            return Calendar.Month(self.now)

        @property
        def year(self):
            """Get current year."""
            return Calendar.Year(self.now)

        @property
        def local(self):
            """Set local timezone."""
            self.timezone = None
            return self

        @property
        def utc(self):
            """Set UTC timezone."""
            self.timezone = dt.timezone.utc
            return self

        @property
        def yd(self):
            """Shortcut for yesterday."""
            return self.yesterday

        @property
        def tm(self):
            """Shortcut for tomorrow."""
            return self.tomorrow

        @property
        def hh(self):
            """Shortcut for hour."""
            return self.hour

        @property
        def mm(self):
            """Shortcut for month."""
            return self.month

        @property
        def yyyy(self):
            """Shortcut for year."""
            return self.year

        @property
        def pv(self):
            """Shrotcut for prev."""
            return self.prev

        @property
        def nt(self):
            """Shrotcut for next."""
            return self.next

        pass

    class Day(Date):
        """Represents certain day."""

        def __init__(self, now):
            super().__init__(now)
            pass

        @property
        def _start(self):
            """Get start of current date."""
            return self._now.replace(hour=0, minute=0, second=0)

        @property
        def _end(self):
            """Get end of current date."""
            return self._now.replace(hour=23, minute=59, second=59)

    pass

    class Today(Day):
        """Represents current day."""

        def __init__(self):
            super().__init__(dt.datetime.now())
            pass

        pass

    class Yesterday(Day):
        """Represents yesterday."""

        def __init__(self, now=None):
            super().__init__((now or dt.datetime.now())-dt.timedelta(days=1))
            pass

        pass

    class Tomorrow(Day):
        """Represents tomorrow."""

        def __init__(self, now=None):
            super().__init__((now or dt.datetime.now())+dt.timedelta(days=1))
            pass

        pass

    class Hour(Date):
        """Represents certain hour."""

        @property
        def _start(self):
            """Get hour start."""
            return self._now.replace(minute=0, second=0)

        @property
        def _end(self):
            """Get hour end."""
            return self._now.replace(minute=59, second=59)

        @property
        def prev(self):
            """Get previous hour."""
            now = self._now-dt.timedelta(hours=1)
            return Calendar.Hour(now)

        @property
        def next(self):
            """Get next hour."""
            now = self._now+dt.timedelta(hours=1)
            return Calendar.Hour(now)

        pass

    class Month(Date):
        """Represents certain month."""

        @property
        def _start(self):
            """Get month start."""
            return self._now.replace(day=1, hour=0, minute=0, second=0)

        @property
        def _end(self):
            """Get month end."""
            day = cnd.monthrange(self._now.year, self._now.month)[1]
            return self._now.replace(day=day, hour=23, minute=59, second=59)

        @property
        def prev(self):
            """Get previous month."""
            now = self._now.replace(day=1)-dt.timedelta(days=1)
            return Calendar.Month(now)

        pass

    class Year(Date):
        """Represents certain year."""

        @property
        def _start(self):
            """Get year start."""
            return self._now.replace(month=1, day=1, hour=0,
                                     minute=0, second=0)

        @property
        def _end(self):
            """Get year end."""
            return self._now.replace(month=12, day=31, hour=23,
                                     minute=59, second=59)

        @property
        def prev(self):
            """Get previous year."""
            now = self._now-dt.timedelta(days=365)
            return Calendar.Year(now)

        pass

    pass


###############################################################################
#                      Module level configuration objects
###############################################################################
config = Configurator()
config.load([user_config, home_config, local_config])

connector = Connector()
connector.load()

calendar = Calendar()

###############################################################################
#                              Default parameters
###############################################################################
DEBUG = False

LOG_CONSOLE = True
LOG_FILE = True
LOG_INFO = True
LOG_DEBUG = False
LOG_ERROR = True
LOG_WARNING = True
LOG_CRITICAL = True
LOG_ALARMING = False
LOG_MAXSIZE = 10485760
LOG_MAXDAYS = 1

DATABASE_VENDOR = None
DATABASE_DRIVER = None
DATABASE_PATH = None
DATABASE_HOST = None
DATABASE_PORT = None
DATABASE_SID = None
DATABASE_SERVICE = None
DATABASE_USER = None
DATABASE_PASSWORD = None

EMAIL_TOGGLE = False
EMAIL_HOST = None
EMAIL_PORT = None
EMAIL_TLS = False
EMAIL_ADDRESS = None
EMAIL_USER = None
EMAIL_PASSWORD = None

API_HOST = None
API_PORT = None
API_TOKEN = None

EDITOR = None
OWNER = None

SCHEDULER_NAME = None
SCHEDULER_DESC = None
SCHEDULER_CHARGERS = 5
SCHEDULER_EXECUTORS = 20
SCHEDULER_RESCHEDULE = 60
SCHEDULER_RERUN = 3600

ENV_PYTHON = sys.executable
