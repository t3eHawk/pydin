"""Contains main application configurator and other configuration elements."""

import importlib as il
import ftplib
import configparser
import os
import sys

import paramiko as po
import pepperoni as pe
import sqlalchemy as sa


home = os.environ.get('DEVOE_HOME')
filename = 'devoe.ini'
user_config = os.path.abspath(os.path.expanduser(f'~/.devoe/{filename}'))
home_config = os.path.join(home, filename) if home is not None else None
local_config = os.path.join(os.path.dirname(sys.argv[0]), filename)
nodes_config = os.path.abspath(os.path.expanduser(f'~/.devoe/nodes.ini'))


class Configurator(dict):
    """Represents application configurator."""

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
    """Represents a special configurator for the tasks and steps loggers."""

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
                self.database = nodemaker.create(database)
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
                       sa.Column('records_merged', sa.Integer,
                                 comment='Total number of merged records'),
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
                self.database = nodemaker.create(database)
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
                       sa.Column('records_merged', sa.Integer,
                                 comment='Total number of merged records'),
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

    pass


class Server():
    """Represents ordinary remote server and connection configuration to it."""

    def __init__(self, host=None, port=None, user=None,
                 password=None, key=None, keyfile=None,
                 ssh=False, sftp=False, ftp=False):
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
        pass

    def connect(self):
        """Connect to remote server."""
        server = self
        creds = self.creds
        return Connection(server, creds)

    pass


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

    pass


class Database(pe.Database):
    """Represents database server and connection configuration to it."""

    def __init__(self, vendor=None, driver=None, path=None,
                 host=None, port=None, sid=None, service=None,
                 user=None, password=None):
        super().__init__(vendor=vendor, driver=driver,
                         path=path, host=host, port=port,
                         sid=sid, service=service,
                         user=user, password=password)
        pass

    pass


class NodeMaker(dict):
    """Represents special configurator used for node connections."""

    def __init__(self):
        super().__init__()
        pass

    class Node():
        """Represents remote node."""

        def __new__(self, **options):
            """Create node instance using options passed."""
            if options.get('database') is True:
                return Database(vendor=options['vendor'],
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
                return Server(host=options.get('host'),
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
        path = path or nodes_config
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
                                'vendor', 'driver', 'path', 'sid', 'service'):
                    self[section][option] = value
        return self

    def create(self, name):
        """Generate Node instance using configuration found by record name."""
        if isinstance(name, str):
            name = name.lower()
            try:
                options = self[name]
            except KeyError as error:
                path = nodes_config
                message = f'no record with name <{name}> found, check {path}'
                traceback = error.__traceback__
                raise Exception(message).with_traceback(traceback)
            else:
                return self.Node(**options)
        else:
            raise TypeError(f'name must be str, not {name.__class__.__name__}')
        pass

    pass


config = Configurator()
config.load([user_config, home_config, local_config])

nodemaker = NodeMaker()
nodemaker.load()


# DEFAULT PARAMETERS
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
