"""Utilits."""

import os
import sys
import signal
import ctypes

import re
import datetime as dt
import calendar as cnd

import abc
import configparser

import threading as th
import subprocess as sp
import importlib as imp

import pepperoni as pe

import sqlalchemy as sa
import sqlalchemy.dialects as sad
import sqlparse as spa

from .sources import Server, Database
from .const import LINUX, MACOS, WINDOWS
from .wrap import classproperty, staticproperty


def locate():
    """Get current location and move to it.

    Returns
    -------
    root : str
        Path to current location as a simple string.
    """
    root = os.path.abspath(os.path.dirname(sys.argv[0]))
    os.chdir(root)
    return root


def declare(object):
    """Declare some object as a module level attribute.

    Returns
    -------
    value : bool
        Always True value meaning that object was successfully declared.
    """
    namespace = imp.import_module('pydin')
    name = object.__class__.__name__.lower()
    setattr(namespace, name, object)
    return True


def cache(object):
    """Save object in application cache.

    Parameters
    ----------
    object : any
        Object that must be registered in memory.

    Returns
    -------
    value : bool
        Always True value indicating that object was successfully stored.
    """
    namespace = imp.import_module('pydin.cache')
    name = object.__class__.__name__.lower()
    setattr(namespace, name, object)
    return True


def importer(module_name, *args):
    """Get importing object from the given module."""
    namespace = imp.import_module(module_name)
    degree = len(args)
    for i, attribute_name in enumerate(args, start=1):
        attribute = getattr(namespace, attribute_name)
        if i < degree:
            namespace = attribute
        elif callable(attribute):
            return attribute()
        else:
            return attribute
    return namespace


def terminator(pid):
    """Terminate process at a given PID correctly."""
    if LINUX or MACOS:
        os.kill(pid, signal.SIGTERM)
    elif WINDOWS:
        kernel = ctypes.windll.kernel32
        kernel.FreeConsole()
        kernel.AttachConsole(pid)
        kernel.SetConsoleCtrlHandler(None, 1)
        kernel.GenerateConsoleCtrlEvent(0, 0)


def case(value, *options):
    """Parse the given arguments into a case-logic."""
    n = len(options)
    for i, option in enumerate(options):
        if i % 2 == 0 and i < n-1 and value == option:
            return options[i+1]
        elif i % 2 == 0 and i == n-1:
            return option


def coalesce(*args):
    """Pick first non-None value from the list of variables.

    Parameters
    ----------
    args
        List of varialbes to check.

    Returns
    -------
    value
        First met non-None value.
    """
    for arg in args:
        if arg is not None:
            return arg


def first(array):
    """Get the first value from the array."""
    if array:
        if isinstance(array, dict):
            key_of_first_value = list(array)[0]
            return array[key_of_first_value]
        elif isinstance(array, (list, tuple)):
            return array[0]
        elif isinstance(array, set):
            return list(array)[0]


def last(array):
    """Get the last value from the array."""
    if array:
        if isinstance(array, dict):
            key_of_last_value = list(array)[-1]
            return array[key_of_last_value]
        elif isinstance(array, (list, tuple)):
            return array[-1]
        elif isinstance(array, set):
            return list(array)[-1]


def to_integer(value):
    """."""
    if isinstance(value, int):
        return value
    elif isinstance(value, float):
        return int(value)
    elif isinstance(value, str) and value.isdigit():
        return int(value)
    elif value is None:
        return value


def to_boolean(value):
    """."""
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return True if value == 'Y' else False
    else:
        return None


def to_none(value):
    """."""
    if not isinstance(value, bool):
        return None


def to_timestamp(value):
    """Convert initial value to timestamp.

    Parameters
    ----------
    value : int, float, str, datetime or None
        Initial value that must be converted.
        Note that str must be in ISO format!

    Returns
    -------
    value : int
        Converted value.
    """
    if isinstance(value, (int, float)) is True:
        return int(value)
    elif isinstance(value, str) is True:
        return int(dt.datetime.fromisoformat(value).timestamp())
    elif isinstance(value, dt.datetime) is True:
        return int(value.timestamp())
    elif value is None:
        return value


def to_date(value):
    """Convert initial value to date object."""
    if isinstance(value, str) is True:
        return dt.date.fromisoformat(value)
    elif isinstance(value, dt.date) is True:
        return value
    elif value is None:
        return value


def to_datetime(value):
    """Convert initial value to datetime object."""
    if isinstance(value, (int, float)) is True:
        return dt.datetime.fromtimestamp(to_timestamp(value))
    elif isinstance(value, str) is True:
        return dt.datetime.fromisoformat(value).replace(microsecond=0)
    elif isinstance(value, dt.datetime) is True:
        return value.replace(microsecond=0)
    elif value is None:
        return value


def to_thread(name, function):
    """Initialize and return a new thread using given parameters."""
    thread = th.Thread(name=name, target=function, daemon=True)
    return thread


def to_process(exe, path=None, args=None, env=None, piped=False):
    """Run the command in a separate process."""
    command = []
    if re.match(r'^.*(\\|/).*$', exe):
        exe = os.path.abspath(exe)
    command.append(exe)
    if path:
        path = os.path.abspath(path)
        command.append(path)
    if args:
        if isinstance(args, str):
            args = args.split()
        command.extend(args)
    kwargs = {}
    kwargs['env'] = env
    kwargs['stdin'] = sp.PIPE if piped else sp.DEVNULL
    kwargs['stdout'] = sp.PIPE if piped else sp.DEVNULL
    kwargs['stderr'] = sp.PIPE if piped else sp.DEVNULL
    if WINDOWS:
        kwargs['creationflags'] = sp.CREATE_NO_WINDOW
    proc = sp.Popen(command, **kwargs)
    return proc


def to_python(path, args=None):
    """Run the Python script in a separate process."""
    python = sys.executable
    proc = to_process(python, path, args)
    return proc


def to_lower(value):
    """Get the string in lowercase."""
    if isinstance(value, str):
        return value.lower()
    else:
        return value


def to_upper(value):
    """Get the string in uppercase."""
    if isinstance(value, str):
        return value.upper()
    else:
        return value


def camel_to_human(s):
    """Conver CamelCase style into ordinary human text."""
    return ''.join([' '+c if c.isupper() else c for c in s]).lstrip()


def to_sql(text):
    """Format given SQL text."""
    result = spa.format(text, keyword_case='upper',
                        identifier_case='lower',
                        reindent_aligned=True)
    return result


def is_path(string):
    """Check whether given string is a path or not."""
    if string is None:
        return False
    elif os.path.exists(string):
        return True
    elif string.startswith('/'):
        return True
    elif string[1:].startswith(':\\'):
        return True
    elif '/' in string:
        return True
    elif '\\' in string:
        return True
    else:
        return False


def read_file_or_string(value):
    """Read content from file or string."""
    if os.path.isfile(value):
        return open(value, 'r').read()
    else:
        return value


def sql_formatter(query, expand_select=None, expand_where=None,
                  make_subquery=False, make_empty=False,
                  fix_new_lines=False, db=None):
    """Modify the given SQL query."""
    import sqlparse.sql as s
    import sqlparse.tokens as t

    def is_select(token):
        if isinstance(token, s.IdentifierList):
            return True
        return False

    def is_where(token):
        if isinstance(token, s.Where):
            return True
        return False

    def has_where(statement):
        for token in statement:
            if is_where(token):
                return True
        return False

    def get_length(statement):
        if statement.tokens:
            return len(statement.tokens)
        return 0

    def as_subquery(statement, db):
        if db.vendor == 'postgresql':
            return f'select * from (\n{statement}) as s\n'
        else:
            return f'select * from (\n{statement})\n'

    parsed = spa.parse(query)
    statement = parsed[0] if parsed else None

    ws = s.Token(t.Whitespace, ' ')
    nl = s.Token(t.Newline, '\n')
    cm = s.Token(t.Punctuation, ',')
    where = s.Token(t.Keyword, 'where')
    and_ = s.Token(t.Operator, 'and')

    if expand_select:
        expansion = s.Token(t.Other, expand_select)
        for token in statement.tokens:
            if is_select(token):
                tokens = s.TokenList([cm, nl, expansion])
                last_position = get_length(token)
                token.insert_after(last_position, tokens)

    if expand_where:
        expansion = s.Token(t.Other, expand_where)
        if has_where(statement):
            tokens = s.TokenList([ws, expansion, nl, and_, ws])
            for token in statement.tokens:
                if is_where(token):
                    token.insert_after(1, tokens)
        else:
            tokens = s.TokenList([nl, where, ws, expansion, nl])
            last_position = get_length(statement)
            statement.insert_after(last_position, tokens)

    if make_subquery:
        statement = as_subquery(statement, db)
    if make_empty:
        select_from = as_subquery(statement, db)
        statement = f'{select_from} where 1 = 0'

    result = spa.format(str(statement), keyword_case='upper',
                        identifier_case='lower', reindent_aligned=True)
    string = str(result)
    if fix_new_lines:
        if re.match(r'INSERT.+SELECT', string):
            string = string.replace('SELECT', '\nSELECT')
    return string


def sql_preparer(obj, db):
    """Prepare the given object for compilation."""
    if isinstance(obj, dt.datetime):
        if db.vendor == 'oracle':
            string = f'{obj:%Y-%m-%d %H:%M:%S}'
            fmt = 'YYYY-MM-DD HH24:MI:SS'
            return sa.func.to_date(string, fmt)
        elif db.vendor == 'sqlite':
            return f'{obj:%Y-%m-%d %H:%M:%S}'
        else:
            return f'{obj:%Y-%m-%d %H:%M:%S}'
    else:
        return obj


def sql_compiler(obj, db):
    """Compile the given object into SQL expression."""
    try:
        database = getattr(sad, db.vendor)
        if database:
            dialect = database.dialect()
            result = obj.compile(dialect=dialect, compile_kwargs=db.ckwargs)
            return str(result)
        else:
            raise NotImplementedError
    except Exception:
        result = obj.compile(compile_kwargs=db.ckwargs)
        return str(result)


def sql_converter(value, db):
    """Convert the given value into SQL expression."""
    if isinstance(value, int):
        return value
    elif isinstance(value, str):
        return f'\'{value}\''
    elif isinstance(value, dt.datetime):
        if db.vendor == 'oracle':
            string = f'{value:%Y-%m-%d %H:%M:%S}'
            fmt = 'YYYY-MM-DD HH24:MI:SS'
            return f'to_date(\'{string}\', \'{fmt}\')'
        else:
            return f'\'{value:%Y-%m-%d %H:%M:%S}\''
    else:
        return value


def get_version():
    """Get current application version."""
    return importer('pydin', '__version__')


def get_job():
    """Get active application job."""
    return importer('pydin', 'job')


def get_logger():
    """Get application core logger."""
    return importer('pydin', 'logger')


def get_email():
    """Get application email interface."""
    return importer('pydin', 'logger', 'email')


def get_credentials():
    """Get a special data dictionary containing saved credentials."""
    return importer('pepperoni', 'credentials')


def get_system_information():
    """Get a special data dictionary containing system information."""
    return importer('pepperoni', 'sysinfo')


def installed():
    """Check if application is installed and configured."""
    from .config import user_config, config
    if not os.path.exists(user_config):
        return False
    elif not config.get('DATABASE'):
        return False
    elif not config['DATABASE'].get('vendor_name'):
        return False
    elif not config.get('API'):
        return False
    elif not config.get('ENVIRONMENTS'):
        return False
    else:
        return True


class Logging():
    """Represents logging configurator used for special loggers."""

    def __init__(self, task_logging=True, step_logging=True,
                 query_logging=False, file_logging=False,
                 database=None, schema_name=None):
        self.metadata = sa.MetaData()
        attributes = ['task', 'step', 'query', 'file']
        for attribute in attributes:
            arg_name = f'{attribute}_logging'
            arg_value = locals()[arg_name]

            logging_name = attribute.lower()
            logging_type = attribute.capitalize()
            logging_status = True if arg_value else False
            table_name = arg_value if isinstance(arg_value, str) else None
            config = arg_value if isinstance(arg_value, dict) else None
            logging = self.make_logging(logging_type, logging_status,
                                        database=database,
                                        schema_name=schema_name,
                                        table_name=table_name,
                                        config=config)
            setattr(self, logging_name, logging)

    def make_logging(self, logging_type, logging_status, database=None,
                     schema_name=None, table_name=None, config=None):
        """Configure one of the logging items."""
        module = sys.modules[__name__]
        class_name = f'{logging_type}Log'
        constructor = getattr(module, class_name)
        if logging_status:
            table_name = coalesce(table_name, constructor.default_name)
            if config:
                table_name = config.get('table_name', table_name)
                schema_name = config.get('schema_name', schema_name)
                database = config.get('database', database)
        else:
            schema_name, table_name = None, None
            database = None
        return constructor(self, database=database,
                           schema_name=schema_name,
                           table_name=table_name)


class BaseLog(abc.ABC):
    """Represents base interface for table logging."""

    def __init__(self, logging, database=None, schema_name=None,
                 table_name=None):
        if isinstance(logging, Logging):
            self.logging = logging
        else:
            class_name = logging.__class__.__name__
            message = f'logging must be Logging, not {class_name}'
            raise TypeError(message)
        if isinstance(database, Database):
            self.database = database
        elif isinstance(database, str):
            self.database = connector.receive(database)
        else:
            module = imp.import_module('pydin.db')
            self.database = module.db
        schema_name = schema_name if isinstance(schema_name, str) else None
        table_name = table_name if isinstance(table_name, str) else None
        self.schema_name, self.table_name = schema_name, table_name

    def __str__(self):
        class_name = camel_to_human(self.__class__.__name__)
        return f'{class_name}ging [{self.table_name}]'

    def __repr__(self):
        class_name = self.__class__.__name__
        return f'{class_name}ging({self.table_name})'

    @abc.abstractmethod
    def default_name(self):
        """Get the default table name used for this logging."""

    @property
    @abc.abstractmethod
    def columns(self):
        """Get list of columns used in logging."""

    @abc.abstractmethod
    def create_table(self):
        """Create database table used in logging."""
        schema_name = self.schema_name
        table_name = self.table_name
        columns = self.columns
        metadata = self.logging.metadata
        table = sa.Table(table_name, metadata, *columns, schema=schema_name)
        table.create(self.database.engine, checkfirst=True)
        return table

    @abc.abstractmethod
    def setup(self, name, date_column=None):
        """Configure logger."""
        if not hasattr(self, 'logger'):
            logger = pe.logger(name, table=True, console=False, file=False,
                               email=False)
            if self.table_name is None:
                logger.configure(table=False)
            else:
                if not self.database.engine.has_table(self.table_name):
                    self.create_table()
                options = {'database': self.database, 'name': self.table_name}
                if date_column:
                    options['date_column'] = date_column
                logger.configure(db=options)
            self.logger = logger
        return self


class TaskLog(BaseLog):
    """Represents task logging interface."""

    @staticproperty
    def default_name():
        return 'pd_task_history'

    @property
    def columns(self):
        seq_name = f'{self.table_name}_seq'
        columns = [sa.Column('id', sa.Integer, sa.Sequence(seq_name),
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
        return columns

    def create_table(self):
        return super().create_table()

    def setup(self):
        logger_name = 'pydin.task.logger'
        return super().setup(logger_name, date_column='updated')


class StepLog(BaseLog):
    """Represents step logging interface."""

    @staticproperty
    def default_name():
        return 'pd_step_history'

    @property
    def columns(self):
        seq_name = f'{self.table_name}_seq'
        columns = [sa.Column('id', sa.Integer, sa.Sequence(seq_name),
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
        return columns

    def create_table(self):
        return super().create_table()

    def setup(self, step):
        logger_name = f'pydin.step.{step.seqno}.logger'
        return super().setup(logger_name, date_column='updated')


class QueryLog(BaseLog):
    """Represents SQL logger configurator."""

    @staticproperty
    def default_name():
        return 'pd_query_log'

    @property
    def columns(self):
        seq_name = f'{self.table_name}_seq'
        columns = [sa.Column('id', sa.Integer, sa.Sequence(seq_name),
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
        return columns

    def create_table(self):
        return super().create_table()

    def setup(self):
        logger_name = 'pydin.query.logger'
        return super().setup(logger_name)

    def start(self, model):
        """Start query logging."""
        job_id = model.job.id if model.job else None
        run_id = model.job.record.id if model.job else None
        task_id = model.task.id
        step_id = model.step.id
        db_name = model.db.name
        table_name = model.table_name
        query_type = model.query_type or model.model_type
        query_text = model.query_text
        self.logger.root.table.new()
        self.logger.table(job_id=job_id,
                          run_id=run_id,
                          task_id=task_id,
                          step_id=step_id,
                          db_name=db_name,
                          table_name=table_name,
                          query_type=query_type,
                          query_text=query_text,
                          start_date=dt.datetime.now())

    def end(self, model):
        """End query logging."""
        output_rows = model.output_rows
        output_text = model.output_text
        error_text = model.error_text
        error_code = model.error_code
        if not error_code and error_text:
            error_pattern = None
            if model.db.vendor == 'oracle':
                error_pattern = r'\w{3}-\d{3,5}'
            if error_pattern and re.match(error_pattern, error_text):
                error_code = re.search(error_pattern, error_text).group()
        self.logger.table(end_date=dt.datetime.now(),
                          output_rows=output_rows,
                          output_text=output_text,
                          error_code=error_code,
                          error_text=error_text)


class FileLog(BaseLog):
    """Represents file logger configurator."""

    @staticproperty
    def default_name():
        return 'pd_file_log'

    @property
    def columns(self):
        seq_name = f'{self.table_name}_seq'
        columns = [sa.Column('id', sa.Integer, sa.Sequence(seq_name),
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
        return columns

    def create_table(self):
        return super().create_table()

    def setup(self):
        logger_name = 'pydin.file.logger'
        return super().setup(logger_name)

    def start(self, model, fileinfo):
        """Start file logging."""
        self.logger.root.table.new()
        job_id = model.job.id if model.job else None
        run_id = model.job.record.id if model.job else None
        task_id = model.task.id
        step_id = model.step.id
        server_name = fileinfo['server'].host
        file_name = fileinfo['file']
        file_date = fileinfo['mtime']
        file_size = fileinfo['size']
        self.logger.table(job_id=job_id,
                          run_id=run_id,
                          task_id=task_id,
                          step_id=step_id,
                          server_name=server_name,
                          file_name=file_name,
                          file_date=file_date,
                          file_size=file_size,
                          start_date=dt.datetime.now())

    def end(self, model):
        """End file logging."""
        self.logger.table(end_date=dt.datetime.now())


class Calendar():
    """Represents calendar configurator."""

    class Date():
        """Represents a single date."""

        def __init__(self, now=None):
            self.now = now
            self.timezone = None

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

            def __call__(self, obj):
                """Get final date value."""
                datetime = self.func(obj)
                if obj.timezone is not None:
                    return datetime.astimezone(tz=obj.timezone)
                else:
                    return datetime

        @property
        @value
        def now(self):
            """Get current date."""
            return self._now

        @now.setter
        def now(self, value):
            if isinstance(value, dt.datetime) or value is None:
                self._now = value.replace(microsecond=0)

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

    class Day(Date):
        """Represents certain day."""

        def __init__(self, now):
            super().__init__(now)

        @property
        def _start(self):
            """Get start of current date."""
            return self._now.replace(hour=0, minute=0, second=0)

        @property
        def _end(self):
            """Get end of current date."""
            return self._now.replace(hour=23, minute=59, second=59)

    class Today(Day):
        """Represents current day."""

        def __init__(self):
            super().__init__(dt.datetime.now())

    class Yesterday(Day):
        """Represents yesterday."""

        def __init__(self, now=None):
            super().__init__((now or dt.datetime.now())-dt.timedelta(days=1))

    class Tomorrow(Day):
        """Represents tomorrow."""

        def __init__(self, now=None):
            super().__init__((now or dt.datetime.now())+dt.timedelta(days=1))

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

    class Week(Date):
        """Represents certain week."""

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


class Connector(dict):
    """Represents connections configurator."""

    def __init__(self):
        super().__init__()
        self.config = os.path.expanduser('~/.pydin/sources.ini')

    class Connection():
        """Represents connection objects factory."""

        def __new__(cls, name=None, **options):
            """Create connection object using options passed."""
            given_options = options.keys()
            database_options = {'vendor_name', 'driver_name', 'database'}
            server_options = {'protocol', 'host', 'port'}

            if set.intersection(database_options, given_options):
                vendor_name = options['vendor_name']
                driver_name = options.get('driver_name')
                database = options.get('database')
                username = options.get('username')
                password = options.get('password')
                host = options.get('host')
                port = options.get('port')
                path = None
                if is_path(database):
                    path = os.path.abspath(database)
                    database = None

                sid = options.get('sid')
                tnsname = options.get('tnsname')
                database = coalesce(database, sid, tnsname)

                service = options.get('service')
                service_name = options.get('service_name')
                service_name = coalesce(service, service_name)

                return Database(name=name, vendor_name=vendor_name,
                                driver_name=driver_name,
                                user=username, password=password,
                                path=path, host=host, port=port,
                                sid=database, service_name=service_name)
            elif set.intersection(server_options, given_options):
                protocol = options.get('protocol').lower()
                host = options.get('host')
                port = options.get('port')
                username = options.get('username')
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

    def load(self, path=None, encoding=None):
        """Load configuration from file."""
        path = path or self.config
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(path, encoding=encoding)
        for section in parser.sections():
            connection_name = section.lower()
            self[connection_name] = {}
            for option in parser.options(section):
                parameter_name = option.lower()
                parameter_value = parser[section][option]
                self[connection_name][parameter_name] = parameter_value
        return self

    def receive(self, name):
        """Receive connection object using its name."""
        if isinstance(name, str):
            name = name.lower()
            try:
                cache = imp.import_module('pydin.cache')
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
            class_name = name.__class__.__name__
            message = f'name must be str, not {class_name}'
            raise TypeError(message)


calendar = Calendar()
connector = Connector()
connector.load()
