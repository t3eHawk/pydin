"""Utilits."""

import os
import sys
import signal
import ctypes

import re
import datetime as dt

import threading as th
import subprocess as sp
import importlib as imp

import sqlalchemy as sa
import sqlalchemy.dialects as sad

import sqlparse as spa

from .const import LINUX, MACOS, WINDOWS


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
    """."""
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
    pass


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
    pass


def to_date(value):
    """Convert initial value to date object."""
    if isinstance(value, str) is True:
        return dt.date.fromisoformat(value)
    elif isinstance(value, dt.date) is True:
        return value
    elif value is None:
        return value
    pass


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
    pass


def to_process(exe, path=None, args=None, env=None, devnull=False, spawn=True):
    """Run the command in a separate process."""
    command = []
    if re.match(r'^.*(\\|/).*$', exe) is not None:
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
    if LINUX or MACOS:
        if devnull:
            kwargs['stdin'] = sp.DEVNULL if devnull else sp.PIPE
            kwargs['stdout'] = sp.DEVNULL if devnull else sp.PIPE
            kwargs['stderr'] = sp.DEVNULL if devnull else sp.PIPE
    elif WINDOWS:
        if spawn:
            kwargs['creationflags'] = sp.CREATE_NO_WINDOW
    proc = sp.Popen(command, **kwargs)
    return proc


def to_thread(name, function):
    """Initialize and return a new thread using given parameters."""
    thread = th.Thread(name=name, target=function, daemon=True)
    return thread


def to_python(path, args=None):
    """Run the Python script in a separate process."""
    python = sys.executable
    proc = to_process(python, path, args, devnull=True, spawn=True)
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
                  make_subquery=False):
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
                tokens = s.TokenList([cm, expansion])
                last_position = get_length(token)
                token.insert_after(last_position, tokens)

    if expand_where:
        expansion = s.Token(t.Other, expand_where)
        if has_where(statement):
            tokens = s.TokenList([expansion, nl, and_, ws])
            for token in statement.tokens:
                if is_where(token):
                    token.insert_after(1, tokens)
        else:
            tokens = s.TokenList([where, ws, expansion, nl])
            last_position = get_length(statement)
            statement.insert_after(last_position, tokens)

    if make_subquery:
        statement = f'select * from ({statement})'

    text = str(statement)
    result = spa.format(text, keyword_case='upper', identifier_case='lower',
                        reindent_aligned=True)
    return str(result)


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
        elif db.vendor == 'sqlite':
            return f'\'{value:%Y-%m-%d %H:%M:%S}\''
    else:
        return value