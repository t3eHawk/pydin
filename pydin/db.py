"""Contains main application database schema and objects."""

import os
import datetime as dt

import pepperoni as pe
import sqlalchemy as sa
import cx_Oracle as oracle

from .config import config
from .utils import installed
from .utils import to_datetime


class Database(pe.Database):
    """Represents database schema and objects."""

    def __init__(self):
        if config['DATABASE'].get('vendor_name'):
            self.configure()
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
            self.schedule = self.load('pd_schedule')
            self.pipeline_config = self.load('pd_pipeline_config')
            self.node_config = self.load('pd_node_config')
            self.run_history = self.load('pd_run_history')
            self.task_history = self.load('pd_task_history')
            self.step_history = self.load('pd_step_history')
            self.sql_log = self.load('pd_sql_log')
            self.file_log = self.load('pd_file_log')
            self.components = self.load('pd_components')

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
            columns = [c.name for c in table.columns]
            primary_keys = [pk.name for pk in table.primary_key.columns]
            values = {c: None for c in columns if c not in primary_keys}
            insert = table.insert().values(**values)
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

    def configure(self):
        vendor_name = config['DATABASE']['vendor_name']
        driver_name = config['DATABASE'].get('driver_name')
        path = config['DATABASE'].get('path')
        host = config['DATABASE'].get('host')
        port = config['DATABASE'].get('port')
        sid = config['DATABASE'].get('sid')
        service_name = config['DATABASE'].get('service_name')
        username = config['DATABASE'].get('username')
        password = config['DATABASE'].get('password')
        client_path = config['DATABASE'].get('client_path')
        if client_path and vendor_name == 'oracle':
            oracle.init_oracle_client(lib_dir=client_path)
        super().__init__(vendor=vendor_name, driver=driver_name,
                         path=path, host=host, port=port,
                         sid=sid, service=service_name,
                         user=username, password=password)

    def load(self):
        """Load application database schema."""
        self.tables = self.Tables(self)

    def deploy(self):
        """Deploy application database schema."""
        if self.vendor == 'sqlite':
            if not os.path.exists(self.path):
                open(self.path, 'w')
        print('WARNING: Automatic DB schema deployment not implemented yet. '
              'Please deploy the schema yourself using scripts from GitHub.')

    def normalize(self, job_name=None, job_description=None, status=None,
                  monthday=None, hour=None, minute=None, second=None,
                  weekday=None, yearday=None, trigger_id=None,
                  start_date=None, end_date=None,
                  environment=None, arguments=None,
                  timeout=None, parallelism=None,
                  rerun_interval=None, rerun_limit=None, rerun_days=None,
                  rerun_period=None, sleep_period=None, wake_up_period=None,
                  alarm=None, email_list=None, debug=None):
        """Normalize parameters in accordance with their data types."""
        values = {}
        setup = [{'column': 'job_name', 'value': job_name,
                  'types': (str,)},
                 {'column': 'job_description', 'value': job_description,
                  'types': (str,)},
                 {'column': 'status', 'value': status,
                  'types': (bool, ),
                  'norm_func': lambda arg: 'Y' if arg is True else 'N'},
                 {'column': 'monthday', 'value': monthday,
                  'types': (int, str)},
                 {'column': 'hour', 'value': hour,
                  'types': (int, str)},
                 {'column': 'minute', 'value': minute,
                  'types': (int, str)},
                 {'column': 'second', 'value': second,
                  'types': (int, str)},
                 {'column': 'weekday', 'value': weekday,
                  'types': (int, str)},
                 {'column': 'yearday', 'value': yearday,
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
                 {'column': 'parallelism', 'value': parallelism,
                  'types': (int, str)},
                 {'column': 'rerun_interval', 'value': rerun_interval,
                  'types': (int,)},
                 {'column': 'rerun_limit', 'value': rerun_limit,
                  'types': (int,)},
                 {'column': 'rerun_days', 'value': rerun_days,
                  'types': (int,)},
                 {'column': 'rerun_period', 'value': rerun_period,
                  'types': (int, str)},
                 {'column': 'sleep_period', 'value': sleep_period,
                  'types': (int, str)},
                 {'column': 'wake_up_period', 'value': wake_up_period,
                  'types': (int, str)},
                 {'column': 'alarm', 'value': alarm,
                  'types': (bool,),
                  'norm_func': lambda arg: 'Y' if arg is True else self.null},
                 {'column': 'email_list', 'value': email_list,
                  'types': (str,)},
                 {'column': 'debug', 'value': debug,
                  'types': (bool,),
                  'norm_func': lambda arg: 'Y' if arg is True else self.null}]

        for item in setup:
            column = item['column']
            value = item['value']
            types = item['types']
            norm_func = item.get('norm_func')
            if isinstance(value, types) or value is self.null:
                if value is self.null or not callable(norm_func):
                    values[column] = value
                elif callable(norm_func):
                    values[column] = norm_func(value)
        return values


db = Database()
if installed(): db.load()