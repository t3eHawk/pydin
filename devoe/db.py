"""Contains main application database schema and objects."""

import datetime as dt

import pepperoni as pe
import sqlalchemy as sa

from .config import config
from .utils import to_datetime


class Database(pe.Database):
    """Represents database schema and objects."""

    def __init__(self):
        if config['DATABASE'].get('vendor'):
            vendor = config['DATABASE']['vendor']
            driver = config['DATABASE'].get('driver')
            path = config['DATABASE'].get('path')
            host = config['DATABASE'].get('host')
            port = config['DATABASE'].get('port')
            sid = config['DATABASE'].get('sid')
            service = config['DATABASE'].get('service')
            user = config['DATABASE'].get('user')
            password = config['DATABASE'].get('password')
            super().__init__(vendor=vendor, driver=driver,
                             path=path, host=host, port=port,
                             sid=sid, service=service,
                             user=user, password=password)
        self.tables = self.Tables(self)
        self.null = self.Null()
        pass

    def __repr__(self):
        """Represent this database as path, SID or service name."""
        if self.path is not None:
            return f'Database[{self.path}]'
        elif self.sid is not None:
            return f'Database[{self.sid}]'
        elif self.service is not None:
            return f'Database[{self.service}]'

    class Tables():
        """Represents database tables."""

        def __init__(self, database):
            self.database = database
            pass

        @property
        def schedule(self):
            """SCHEDULE table."""
            if hasattr(self, '_schedule') is False:
                self._schedule = self.database.table('de_schedule')
            return self._schedule

        @property
        def history(self):
            """HISTORY table."""
            if hasattr(self, '_history') is False:
                self._history = self.database.table('de_history')
            return self._history

        @property
        def components(self):
            """COMPONENTS table."""
            if hasattr(self, '_components') is False:
                self._components = self.database.table('de_components')
            return self._components

    class Null(sa.sql.elements.Null):
        """Represent null data type."""

        def __repr__(self):
            """Represent as NULL."""
            return 'NULL'

        pass

    def deploy(self):
        """Deploy application database schema."""
        raise NotImplementedError
        pass

    def normalize(self, job_name=None, job_desc=None, status=None,
                  monthday=None, weekday=None,
                  hour=None, minute=None, second=None, trigger_id=None,
                  start_date=None, end_date=None,
                  environment=None, arguments=None, timeout=None,
                  reruns=None, days_rerun=None,
                  alarm=None, persons=None, debug=None):
        """Normalize parameters in accordance with their data types."""
        values = {}
        setup = [{'column': 'job_name', 'value': job_name,
                  'types': (str,)},
                 {'column': 'job_desc', 'value': job_desc,
                  'types': (str,)},
                 {'column': 'status', 'value': status,
                  'types': (bool, ),
                  'norm_func': lambda arg: 'Y' if arg is True else 'N'},
                 {'column': 'monthday', 'value': monthday,
                  'types': (int, str)},
                 {'column': 'weekday', 'value': weekday,
                  'types': (int, str)},
                 {'column': 'hour', 'value': hour,
                  'types': (int, str)},
                 {'column': 'minute', 'value': min,
                  'types': (int, str)},
                 {'column': 'second', 'value': second,
                  'types': (int, str)},
                 {'column': 'trigger_id', 'value': trigger_id,
                  'types': (int,)},
                 {'column': 'start_date', 'value': start_date,
                  'types': (str, dt.datetime), 'norm_func': to_datetime},
                 {'column': 'end_date', 'value': end_date,
                  'types': (str, dt.datetime), 'norm_func': to_datetime},
                 {'column': 'environment', 'value': environment,
                  'types': (str,)},
                 {'column': 'arguments', 'value': arguments,
                  'types': (str,)},
                 {'column': 'timeout', 'value': timeout,
                  'types': (int,)},
                 {'column': 'reruns', 'value': reruns,
                  'types': (int,)},
                 {'column': 'days_rerun', 'value': days_rerun,
                  'types': (int,)},
                 {'column': 'alarm', 'value': alarm,
                  'types': (bool,),
                  'norm_func': lambda arg: 'Y' if arg is True else self.null},
                 {'column': 'persons', 'value': persons,
                  'types': (str,)},
                 {'column': 'debug', 'value': debug,
                  'types': (bool,),
                  'norm_func': lambda arg: 'Y' if arg is True else self.null}]

        for item in setup:
            column = item['column']
            value = item['value']
            types = item['types']
            norm_func = item.get('norm_func')
            if isinstance(value, (*types, self.Null)) is True:
                if (
                    isinstance(value, self.Null) is True
                    or callable(norm_func) is False
                   ):
                    values[column] = value
                elif callable(norm_func) is True:
                    values[column] = norm_func(value)
        return values


db = Database()
