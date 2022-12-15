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

from .utils import coalesce
from .utils import is_path


home = os.environ.get('PYDIN_HOME')
filename = 'pydin.ini'
user_config = os.path.abspath(os.path.expanduser(f'~/.pydin/{filename}'))
home_config = os.path.join(home, filename) if home is not None else None
local_config = os.path.join(os.path.dirname(sys.argv[0]), filename)


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

    def save(self):
        """Save current settings to the user configuration file."""
        config_parser = configparser.ConfigParser()
        config_dict = {'GENERAL': self['GENERAL'],
                       'DATABASE': self['DATABASE'],
                       'EMAIL': self['EMAIL'],
                       'API': self['API'],
                       'ENVIRONMENTS': self['ENVIRONMENTS']}
        config_parser.read_dict(config_dict)
        with open(user_config, 'w') as fh:
            config_parser.write(fh, space_around_delimiters=False)

    def has_table(self, table):
        """Check if configuration table exists."""
        if self.get(table) is not None:
            return True
        else:
            return False
        pass


class Logging():
    """Represents logging configurator used for special loggers."""

    def __init__(self, task=True, step=True, file=False, sql=False,
                 database=None, schema=None, table=None):
        self.metadata = sa.MetaData()

        if task is True:
            self.task = self.Task(self, table='pd_task_history')
        elif task is False:
            self.task = self.Task(self, table=None)
        elif isinstance(task, dict):
            self.task = self.Task(self, table=task.get('table', table),
                                  schema=task.get('schema', schema),
                                  database=task.get('database', database))
        else:
            table = table if not isinstance(task, str) else task
            self.task = self.Task(self, table=table, schema=schema,
                                  database=database)

        if step is True:
            self.step = self.Step(self, table='pd_step_history')
        elif step is False:
            self.step = self.Step(self, table=None)
        elif isinstance(step, dict):
            self.step = self.Step(self, table=step.get('table'),
                                  schema=step.get('schema', schema),
                                  database=step.get('database', database))
        else:
            table = None if not isinstance(step, str) else step
            self.step = self.Step(self, table=table, schema=schema,
                                  database=database)

        if file is True:
            self.file = self.File(self, table='pd_file_log')
        elif file is False:
            self.file = self.File(self, table=None)
        elif isinstance(file, dict):
            self.file = self.File(self, table=file.get('table'),
                                  schema=file.get('schema', schema),
                                  database=file.get('database', database))
        else:
            table = None if not isinstance(file, str) else file
            self.file = self.File(self, table=table, schema=schema,
                                  database=database)

        if sql is True:
            self.sql = self.SQL(self, table='pd_sql_log')
        elif sql is False:
            self.sql = self.SQL(self, table=None)
        elif isinstance(sql, dict):
            self.sql = self.SQL(self, table=sql.get('table'),
                                schema=sql.get('schema', schema),
                                database=sql.get('database', database))
        else:
            table = None if not isinstance(sql, str) else sql
            self.sql = self.SQL(self, table=table, schema=schema,
                                database=database)
        pass

    class Task():
        """Represents task logger configurator."""

        def __init__(self, logging, database=None, schema=None, table=None):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('pydin.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
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
                       sa.Column('task_name', sa.String(30),
                                 comment='Name of the task'),
                       sa.Column('task_date', sa.DateTime,
                                 comment='Date for which task was performed'),
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
                       sa.Column('records_error', sa.Integer,
                                 comment='Total number of occurred errors'),
                       sa.Column('result_value', sa.Integer,
                                 comment='Short numeric execution result'),
                       sa.Column('result_long', sa.String(512),
                                 comment='Long string execution result'),
                       sa.Column('updated', sa.DateTime,
                                 comment='Date of this record change')]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self):
            """Configure database logger."""
            logger = pe.logger('pydin.task.logger', table=True,
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
        """Represents step logger configurator."""

        def __init__(self, logging, database=None, schema=None, table=None):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('pydin.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
            pass

        def create_table(self):
            """Create database table used for logging."""
            name = self.table
            schema = self.schema
            metadata = self.logging.metadata
            columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                                 primary_key=True,
                                 comment='Unique step ID'),
                       sa.Column('job_id', sa.Integer,
                                 comment='Job that performed this step'),
                       sa.Column('run_id', sa.Integer,
                                 comment='Run that performed this step'),
                       sa.Column('task_id', sa.Integer,
                                 comment='Task ID of this step'),
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
                       sa.Column('step_date', sa.DateTime,
                                 comment='Date for which step was performed'),
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
                       sa.Column('records_error', sa.Integer,
                                 comment='Total number of occurred errors'),
                       sa.Column('result_value', sa.Integer,
                                 comment='Short numeric result of execution'),
                       sa.Column('result_long', sa.String(512),
                                 comment='Long string result of execution'),
                       sa.Column('updated', sa.DateTime,
                                 comment='Date of this record change')]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self, step):
            """Configure database logger."""
            logger = pe.logger(f'pydin.step.{step.seqno}.logger', table=True,
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

    class File():
        """Represents file logger configurator."""

        def __init__(self, logging, database=None, schema=None, table=None):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('pydin.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
            pass

        def create_table(self):
            """Create database table used for logging."""
            name = self.table
            schema = self.schema
            metadata = self.logging.metadata
            columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                                 primary_key=True,
                                 comment='Unique file ID'),
                       sa.Column('job_id', sa.Integer,
                                 comment='Job that processed the file'),
                       sa.Column('run_id', sa.Integer,
                                 comment='Run that processed the file'),
                       sa.Column('task_id', sa.Integer,
                                 comment='Task that processed the file'),
                       sa.Column('step_id', sa.Integer,
                                 comment='Step that processed the file'),
                       sa.Column('server', sa.String(30),
                                 comment='Host name having the file'),
                       sa.Column('file_name', sa.String(256),
                                 comment='Name of the file'),
                       sa.Column('file_date', sa.DateTime,
                                 comment='File modification time'),
                       sa.Column('file_size', sa.Integer,
                                 comment='File size in bytes'),
                       sa.Column('start_date', sa.DateTime,
                                 comment='Date when file process started'),
                       sa.Column('end_date', sa.DateTime,
                                 comment='Date when file process ended')]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self):
            """Configure database logger."""
            logger = pe.logger(f'pydin.file.logger', table=True,
                               console=False, file=False, email=False)
            if self.table is None:
                logger.configure(table=False)
            else:
                if not self.database.engine.has_table(self.table):
                    self.create_table()
                logger.configure(db={'database': self.database,
                                     'name': self.table})
            return logger

        pass

    class SQL():
        """Represents SQL logger configurator."""

        def __init__(self, logging, database=None, schema=None, table=None):
            self.logging = logging if isinstance(logging, Logging) else None
            self.database = database if isinstance(database, str) else None
            if isinstance(database, Database) is True:
                self.database = database
            elif isinstance(database, str):
                self.database = connector.receive(database)
            else:
                module = il.import_module('pydin.db')
                self.database = module.db
            self.schema = schema if isinstance(schema, str) else None
            self.table = table if isinstance(table, str) else None
            pass

        def create_table(self):
            """Create database table used for logging."""
            name = self.table
            schema = self.schema
            metadata = self.logging.metadata
            columns = [sa.Column('id', sa.Integer, sa.Sequence(f'{name}_seq'),
                                 primary_key=True,
                                 comment='Unique file ID'),
                       sa.Column('job_id', sa.Integer,
                                 comment='Job that processed the file'),
                       sa.Column('run_id', sa.Integer,
                                 comment='Run that processed the file'),
                       sa.Column('task_id', sa.Integer,
                                 comment='Task that processed the file'),
                       sa.Column('step_id', sa.Integer,
                                 comment='Step that processed the file'),
                       sa.Column('db_name', sa.String(25),
                                 comment='Database of SQL execution'),
                       sa.Column('table_name', sa.String(128),
                                 comment='Name of involved table'),
                       sa.Column('query_type', sa.String(10),
                                 comment='Type of SQL being executed'),
                       sa.Column('query_text', sa.String(10),
                                 comment='Text of SQL being executed'),
                       sa.Column('start_date', sa.DateTime,
                                 comment='Date when SQL execution started'),
                       sa.Column('end_date', sa.DateTime,
                                 comment='Date when SQL execution ended'),
                       sa.Column('output_rows', sa.Integer,
                                 comment='Total number of returned rows'),
                       sa.Column('output_text', sa.Integer,
                                 comment='Returned database message'),
                       sa.Column('error_code', sa.Integer,
                                 comment='Occurred database error code'),
                       sa.Column('error_text', sa.Integer,
                                 comment='Occurred database error text')]
            table = sa.Table(name, metadata, *columns, schema=schema)
            table.create(self.database.engine, checkfirst=True)
            return table

        def setup(self):
            """Configure database logger."""
            logger = pe.logger(f'pydin.sql.logger', table=True,
                               console=False, file=False, email=False)
            if self.table is None:
                logger.configure(table=False)
            else:
                if not self.database.engine.has_table(self.table):
                    self.create_table()
                logger.configure(db={'database': self.database,
                                     'name': self.table})
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

        cache = il.import_module('pydin.cache')
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

        cache = il.import_module('pydin.cache')
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
        self.config = os.path.expanduser('~/.pydin/sources.ini')
        pass

    class Connection():
        """Represents connection objects factory."""

        def __new__(self, name=None, **options):
            """Create connection object using options passed."""
            given_options = options.keys()
            database_options = {'vendor', 'driver', 'database'}
            server_options = {'protocol', 'host', 'port'}

            if set.intersection(database_options, given_options):
                vendor = options['vendor']
                driver = options.get('driver')
                database = options.get('database')
                username = options.get('username')
                password = options.get('password')
                host = options.get('host')
                port = options.get('port')

                if is_path(database):
                    path = os.path.abspath(database)
                    database = None

                sid = options.get('sid')
                tnsname = options.get('tnsname')
                database = coalesce(database, sid, tnsname)

                service = options.get('service')
                service_name = options.get('service_name')
                service = coalesce(service, service_name)

                return Database(name=name, vendor=vendor, driver=driver,
                                user=username, password=password,
                                path=path, host=host, port=port,
                                sid=database, service=service)
            elif set.intersection(server_options, given_options):
                protocol = options.get('protocol').lower()
                host = options.get('host')
                port = options.get('port')
                username = options.get('user')
                password = options.get('password')
                key = options.get('key')
                keyfile = options.get('keyfile')

                ssh = True if protocol == 'ssh' else False
                sftp = True if protocol == 'sftp' else False
                ftp = True if protocol == 'ftp' else False

                return Server(name=name, host=host, port=port,
                              user=username, password=password,
                              key=key, keyfile=keyfile,
                              ssh=ssh, sftp=sftp, ftp=ftp)
            else:
                return None

        pass

    def load(self, path=None, encoding=None):
        """Load configuration from file."""
        path = path or self.config
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(path, encoding=encoding)
        for section in parser.sections():
            section = section.lower()
            self[section] = {}
            for option in parser.options(section):
                option = option.lower()
                value = parser[section][option]
                self[section][option] = value
        return self

    def receive(self, name):
        """Receive connection object using its name."""
        if isinstance(name, str):
            name = name.lower()
            try:
                cache = il.import_module('pydin.cache')
                return cache.CONNECTIONS[name]
            except KeyError:
                try:
                    options = self[name]
                except KeyError as error:
                    path = self.config
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

        def __getattribute__(self, name):
            if '(' in name and name.endswith(')'):
                func_name = name[:name.index('(')]
                if hasattr(self, func_name):
                    expression = f'self.{name}'
                    attribute = eval(expression)
                    return attribute
                else:
                    return super().__getattribute__(name)
            else:
                return super().__getattribute__(name)

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
        def timezone(self):
            return self._timezone

        @timezone.setter
        def timezone(self, value):
            if value is None or value is dt.timezone.utc:
                self._timezone = value
            elif isinstance(value, int):
                timezone = dt.timezone(dt.timedelta(hours=value))
                self._timezone = timezone
            elif isinstance(value, str):
                if value.upper() == 'UTC':
                    self._timezone = dt.timezone.utc
                elif value.isdigit():
                    hours = int(value)
                    self._timezone = dt.timezone(dt.timedelta(hours=hours))
            elif isinstance(value, (list, tuple)):
                hours, minutes = value[0], value[1]
                timedelta = dt.timedelta(hours=hours, minutes=minutes)
                self._timezone = dt.timezone(timedelta)

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
        def y(self):
            """Shortcut for year."""
            return self.year

        @property
        def dd(self):
            """Shortcut for day."""
            return self

        @property
        def mm(self):
            """Shortcut for month."""
            return self.month

        @property
        def hh(self):
            """Shortcut for hour."""
            return self.hour

        @property
        def pv(self):
            """Shrotcut for prev."""
            return self.prev

        @property
        def nt(self):
            """Shrotcut for next."""
            return self.next

        def days_back(self, delta):
            delta = dt.timedelta(days=delta)
            now = self._now-delta
            return Calendar.Day(now)

        def hours_back(self, delta):
            delta = dt.timedelta(hours=delta)
            now = self._now-delta
            return Calendar.Hour(now)

        def minutes_back(self, delta):
            delta = dt.timedelta(minutes=delta)
            now = self._now-delta
            return Calendar.Day(now)

        def seconds_back(self, delta):
            delta = dt.timedelta(seconds=delta)
            now = self._now-delta
            return Calendar.Day(now)

        def months_back(self, delta):
            now = self.now
            while delta > 0:
                delta -= 1
                now = now.replace(day=1)-dt.timedelta(days=1)
            return Calendar.Month(now)

        def week_back(self, delta):
            pass

        def years_back(self, delta):
            pass

        def minutes_round(self, level):
            date = self._now
            micro = date.microsecond
            seconds = date.second
            minutes = date.minute % level
            delta = dt.timedelta(seconds=seconds, minutes=minutes,
                                 microseconds=micro)
            now = date-delta
            return Calendar.Date(now)

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

    class Week(Date):
        """Represents certain week."""

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
SCHEDULER_CHARGERS_NUMBER = 5
SCHEDULER_EXECUTORS_NUMBER = 20
SCHEDULER_REFRESH_INTERVAL = 300
SCHEDULER_RERUN_DELAY = 14400
SCHEDULER_RERUN_INTERVAL = 3600
SCHEDULER_WAKEUP_INTERVAL = 60

ENV_PYTHON = sys.executable
