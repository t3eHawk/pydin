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
    """Represents special configurator used in table logger of ETL tasks."""

    def __init__(self, database=None, table=None, schema=None,
                 records_read=True, records_written=True,
                 records_updated=True, records_merged=True,
                 files_read=True, files_written=True,
                 bytes_read=True, bytes_written=True,
                 errors_found=True, initiator=True,
                 file_log=False, text_log=False, text_error=False):
        self.metadata = sa.MetaData()
        self.database = database if isinstance(database, str) else None
        self.table = table if isinstance(table, str) else None
        self.schema = schema if isinstance(schema, str) else None

        self.records_read = None
        if isinstance(records_read, bool):
            self.records_read = records_read
        self.records_written = None
        if isinstance(records_written, bool):
            self.records_written = records_written
        self.records_updated = None
        if isinstance(records_updated, bool):
            self.records_updated = records_updated
        self.records_merged = None
        if isinstance(records_merged, bool):
            self.records_merged = records_merged
        self.files_read = None
        if isinstance(files_read, bool):
            self.files_read = files_read
        self.files_written = None
        if isinstance(files_written, bool):
            self.files_written = files_written
        self.bytes_read = None
        if isinstance(bytes_read, bool):
            self.bytes_read = bytes_read
        self.bytes_written = None
        if isinstance(bytes_written, bool):
            self.bytes_written = bytes_written
        self.errors_found = None
        if isinstance(errors_found, bool):
            self.errors_found = errors_found
        self.initiator = None
        if isinstance(initiator, bool):
            self.initiator = initiator
        self.file_log = None
        if isinstance(file_log, bool):
            self.file_log = file_log
        self.text_log = None
        if isinstance(text_log, bool):
            self.text_log = text_log
        self.text_error = None
        if isinstance(text_error, bool):
            self.text_error = text_error
        pass

    def setup(self):
        """Configure database logger."""
        logger = pe.logger('devoe.task.logger', table=True,
                           console=False, file=False, email=False)
        # If no table is mentioned then the logging is off and all writes
        # to it must be omitted.
        if self.table is None:
            logger.configure(table=False)
        else:
            name = self.database
            database = nodemaker.create(name) if name is not None else None
            # If no database provided but table name is mentioned then the
            # application database will be used.
            if database is None:
                module = il.import_module('devoe.db')
                database = module.db
            if not database.engine.has_table(self.table):
                self.create_table(database)
            logger.configure(db={'database': database, 'name': self.table})
        return logger

    def create_table(self, database):
        """Create database table used for logging."""
        vendor = database.vendor
        name = self.table
        schema = self.schema
        metadata = self.metadata
        columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                             primary_key=True, comment='Unique ETL task ID'),
                   sa.Column('job_id', sa.Integer,
                             comment='Job that performed this task'),
                   sa.Column('run_id', sa.Integer,
                             comment='Run that performed this task'),
                   sa.Column('run_date', sa.DateTime,
                             comment='Date for which task was performed'),
                   sa.Column('updated', sa.DateTime,
                             comment='Date of this records change'),
                   sa.Column('start_date', sa.DateTime,
                             comment='Date when this task started'),
                   sa.Column('end_date', sa.DateTime,
                             comment='Date when this task ended'),
                   sa.Column('status', sa.String(1),
                             comment=('Current task status. '
                                      'R - running, D - Done, E - error')),
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
                   sa.Column('initiator', sa.String(30),
                             comment='OS user who initiated this task'),
                   sa.Column('file_log', sa.String(30),
                             comment='Name of log file used for this task'),
                   sa.Column('text_log', sa.Text,
                             comment='Textual log of this task'),
                   sa.Column('text_error', sa.Text,
                             comment='Textual error that happened in task')]
        columns = [column for column in columns
                   if getattr(self, column.name, True) is True]
        table = sa.Table(name, metadata, *columns, schema=schema,
                         oracle_compress=True if vendor == 'oracle' else None)
        table.create(database.engine, checkfirst=True)
        return table


class Localhost():
    """Represents localhost i.e. no connection required."""

    def __init__(self):
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
