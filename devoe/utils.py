"""Utilits."""

import datetime as dt
import importlib as imp
import os
import re
import subprocess as sp
import sys

import sqlparse

from .logger import logger


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


def register(object):
    """Register certain object in memory as a part of application.

    Parameters
    ----------
    object : any
        Object that must be registered in memory.

    Returns
    -------
    value : bool
        Always True value indicating that object was successfully stored.
    """
    cache = imp.import_module('devoe.cache')
    name = object.__class__.__name__
    setattr(cache, name, object)
    return True


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
    if isinstance(value, dt.date) is True:
        return value
    if value is None:
        return value


def to_datetime(value):
    """Convert initial value to datetime object."""
    if isinstance(value, str) is True:
        return dt.datetime.fromisoformat(value).replace(microsecond=0)
    if isinstance(value, dt.datetime) is True:
        return value.replace(microsecond=0)
    if value is None:
        return value


def to_process(exe, file=None, args=None, env=None, devnull=False, spawn=True):
    """Run the command in a separate process."""
    logger.debug(f'Making process from {exe=}, {file=}, {args=}')
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
    logger.debug(f'{command=}')
    kwargs = {}
    kwargs['env'] = env
    kwargs['stdout'] = sp.DEVNULL if devnull is True else sp.PIPE
    kwargs['stderr'] = sp.DEVNULL if devnull is True else sp.PIPE
    if os.name == 'nt' and spawn is True:
        kwargs['creationflags'] = sp.CREATE_NO_WINDOW
    proc = sp.Popen(command, **kwargs)
    return proc


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
