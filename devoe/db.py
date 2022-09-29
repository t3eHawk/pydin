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
            self.schedule = self.load('de_schedule')
            self.components = self.load('de_components')
            self.run_history = self.load('de_run_history')
            self.task_history = self.load('de_task_history')
            self.step_history = self.load('de_step_history')
            self.sql_log = self.load('de_sql_log')
            self.file_log = self.load('de_file_log')
            self.job_config = self.load('de_job_config')
            pass

        def load(self, table_name=None):
            if table_name:
                return self.database.table(table_name)

        pass

    class Record():
        """."""

        def __init__(self, database, table, id=None):
            self.database = database
            self.table = table
            self.id = id
            pass

        def select(self, id):
            """."""
            self.id = id
            return self.id

        def create(self):
            """."""
            conn = db.connect()
            table = self.table
            insert = table.insert()
            result = conn.execute(insert)
            self.id = result.inserted_primary_key[0]
            return self.id

        def read(self):
            """."""
            if self.id:
                conn = self.database.connect()
                table = self.table
                select = table.select().where(table.c.id == self.id)
                result = conn.execute(select).first()
                return result
            else:
                return None
            pass

        def write(self, **kwargs):
            """."""
            if self.id:
                conn = self.database.connect()
                table = self.table
                updated = dt.datetime.now()
                update = (table.update().values(updated=updated, **kwargs).
                          where(table.c.id == self.id))
                result = conn.execute(update)
                return result
            else:
                return None
            pass

        pass

    class Null(sa.sql.elements.Null):
        """Represent null data type."""

        def __repr__(self):
            """Represent as NULL."""
            return 'NULL'

        pass

    def record(self, table, id=None):
        """."""
        return self.Record(self, table, id=id)

    def deploy(self):
        """Deploy application database schema."""
        raise NotImplementedError

    def normalize(self, job=None, description=None, status=None,
                  monthday=None, weekday=None,
                  hour=None, minute=None, second=None, trigger_id=None,
                  start_date=None, end_date=None,
                  environment=None, arguments=None, timeout=None,
                  maxreruns=None, maxdays=None,
                  alarm=None, recipients=None, debug=None):
        """Normalize parameters in accordance with their data types."""
        values = {}
        setup = [{'column': 'job', 'value': job,
                  'types': (str,)},
                 {'column': 'description', 'value': description,
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
                 {'column': 'maxreruns', 'value': maxreruns,
                  'types': (int,)},
                 {'column': 'maxdays', 'value': maxdays,
                  'types': (int,)},
                 {'column': 'alarm', 'value': alarm,
                  'types': (bool,),
                  'norm_func': lambda arg: 'Y' if arg is True else self.null},
                 {'column': 'recipients', 'value': recipients,
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
