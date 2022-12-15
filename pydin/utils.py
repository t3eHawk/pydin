"""Utilits."""


import os
import sys

import re
import datetime as dt

import threading as th
import subprocess as sp
import importlib as imp

import sqlparse


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


def declare(object):
    """."""
    namespace = imp.import_module('pydin')
    name = object.__class__.__name__.lower()
    setattr(namespace, name, object)
    return True


def installed():
    """Check if application is installed and configured."""
    from .config import user_config, local_config, config
    if not os.path.exists(user_config):
        return False
    elif not os.path.exists(local_config):
        return False
    elif not config.get('DATABASE'):
        return False
    elif not config['DATABASE'].get('vendor'):
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


def to_process(exe, file=None, args=None, env=None, devnull=False, spawn=True):
    """Run the command in a separate process."""
    # logger.debug(f'Making process from {exe=}, {file=}, {args=}')
    command = []
    if re.match(r'^.*(\\|/).*$', exe) is not None:
        exe = os.path.abspath(exe)
    command.append(exe)
    if file is not None:
        file = os.path.abspath(file)
        command.append(file)
    if args is not None:
        if isinstance(args, str) is True:
            args = args.split()
        command.extend(args)
    # logger.debug(f'{command=}')
    kwargs = {}
    kwargs['env'] = env
    kwargs['stdout'] = sp.DEVNULL if devnull is True else sp.PIPE
    kwargs['stderr'] = sp.DEVNULL if devnull is True else sp.PIPE
    if os.name == 'nt' and spawn is True:
        kwargs['creationflags'] = sp.CREATE_NO_WINDOW
    proc = sp.Popen(command, **kwargs)
    return proc


def to_thread(name, function):
    """Initialize and return a new thread using given parameters."""
    thread = th.Thread(name=name, target=function, daemon=True)
    return thread


def to_python(file, args=None):
    """Run the Python script in a separate process."""
    python = sys.executable
    proc = to_process(python, file, args, devnull=True, spawn=True)
    return proc


def to_sql(text):
    """Format given SQL text."""
    result = sqlparse.format(text, keyword_case='upper',
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