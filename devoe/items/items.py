import csv
import datetime as dt
import gzip
import os
import re
import shutil
import stat
import sqlalchemy as sql
import tempfile
import threading

from ..config import make_config
from ..dates import Today
from ..log import make_log
from ..meta import (Extractable, Loadable, Executable, Unextractable,
                    Unloadable, Unexecutable, Fetchable)
from ..nodes.database import Database, make_database
from ..nodes.host import Remote, Localhost, make_remote
from ..step import Step
from ..sql.dml import merge
from ..task import Task


class Item():

    def __init__(self, title=None, desc=None, bind=None, config=None):
        # Item logger.
        self.log = make_log()

        # Identity variables.
        self.title = title or self.__class__.__name__
        self.desc = desc or f'ETL <{self.__class__.__name__}>'
        # Pipeline that include that item.
        self.bind = bind
        # Configurator.
        self.config = config
        # Step that works with item as current moment.
        self.step = None

        # Item that goes before in the pipeline.
        self.source = None
        # Items that goes after in the pipeline.
        self.targets = []
        pass

    def __str__(self):
        return f'<{self.title}>'

    __repr__ = __str__

    @property
    def bind(self):
        return self._bind

    @bind.setter
    def bind(self, value):
        if value is None:
            self._bind = None
        elif isinstance(value, Task) is True:
            self._bind = value
        else:
            raise TypeError(f'must be Task, not {value.__class__.__name__}')
        pass

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        if isinstance(value, (str, dict)) is True:
            self._config = value
        else:
            self._config = None

    @property
    def step(self):
        return self._step

    @step.setter
    def step(self, value):
        if value is None:
            self._step = None
        elif isinstance(value, Step) is True:
            self._step = value
        else:
            raise TypeError(f'must be Step, not {value.__class__.__name__}')
        pass

    def assign(self, bind=None, step=None):
        if bind is not None:
            self.bind = bind
        if step is not None:
            self.step = step
        return self

    def prepare(self):
        pass

    def extract(self):
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.extractor, daemon=True,
                                       name=f'Extract Thread {self}')
        self.log.debug(f'{self.thread.name} starting')
        self.thread.start()
        pass

    def load(self):
        self.prepare()
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.loader, daemon=True,
                                       name=f'Load Thread {self}')
        self.log.debug(f'{self.thread.name} starting')
        self.thread.start()
        pass

    def stop(self):
        self.event.set()
        self.log.debug(f'{self.thread.name} closed')
        self.thread.join()
        pass

class Table(Item, Extractable, Loadable, Unexecutable, Fetchable):
    """This class represents a table object with its description."""

    def __init__(self, database=None, name=None, schema=None, dblink=None,
                 columns=None, primary_key=None, foreign_keys=None,
                 compress=False, fetch=1000, commit=1000, purge=False,
                 append=False, title=None, desc=None, bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind, config=config)
        self.database = self.db = None
        self.proxy = None
        self.name = None
        self.schema = None
        self.dblink = None
        self.columns = []
        self.primary_key = None
        self.foreign_keys = []
        self.compress = None
        self.fetch = None
        self.commit = None
        self.append = None
        self.purge = None
        self.answerset = None
        self.selected = 0
        self.inserted = 0
        self.updated = 0
        self.merged = 0
        self.counted = 0

        self.configure(database=database, name=name, schema=schema,
                       dblink=dblink, columns=columns, primary_key=primary_key,
                       foreign_keys=foreign_keys, compress=compress,
                       fetch=fetch, commit=commit, purge=purge, append=append)
        pass

    @property
    def exists(self):
        if self.db is not None:
            return self.db.engine.has_table(self.name)

    def configure(self, database=None, name=None, schema=None, dblink=None,
                  columns=None, primary_key=None, foreign_keys=None,
                  compress=None, select_all=None, distinct=None, alias=None,
                  joins=None, fetch=None, commit=None, parallel=None,
                  purge=None, append=None, config=None):
        # Process config if exists.
        if isinstance(config, (str, dict)) is True:
            self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            database = config.get('database', database)
            name = config.get('name', name)
            schema = config.get('schema', schema)
            dblink = config.get('dblink', dblink)
            columns = config.get('columns', columns)
            primary_key = config.get('primary_key', primary_key)
            foreign_keys = config.get('foreign_keys', foreign_keys)
            compress = config.get('compress', compress)
            fetch = config.get('fetch', dblink)
            commit = config.get('commit', commit)
            purge = config.get('purge', purge)

        # Database which stores that table.
        if isinstance(database, Database) is True:
            self.database = self.db = database
        elif isinstance(database, str) is True:
            self.database = self.db = make_database(database)

        # Identification.
        if isinstance(name, str) is True:
            self.name = name.lower()
        if isinstance(schema, str) is True:
            self.schema = schema.lower()
        if isinstance(dblink, str) is True:
            self.dblink = dblink.lower()

        # The definition related stuff.
        if isinstance(columns, list) is True:
            self.columns = columns
        if isinstance(primary_key, (list, dict)) is True:
            self.primary_key = primary_key
        if isinstance(foreign_keys, list) is True:
            self.foreign_keys = foreign_keys
        if isinstance(compress, bool) is True:
            self.compress = compress

        # The query related stuff.
        if isinstance(purge, bool) is True:
            self.purge = purge
        if isinstance(append, bool) is True:
            self.append = append
        if isinstance(fetch, int) is True:
            self.fetch = fetch
        if isinstance(commit, int) is True:
            self.commit = commit
        pass

    def prepare(self):
        if self.exists is False:
            self.create()
        else:
            self.get()
            if self.purge is True:
                self.delete()
        pass

    def extractor(self):
        self.select()
        self.log.info('Fetching records...')
        while True:
            chunk = self.fetchmany()
            self.log.set(records_read=self.selected)
            if chunk:
                self.step.queue.put(chunk)
            else:
                break
        pass

    def loader(self):
        while True:
            if self.step.queue.empty() is False:
                chunk = self.step.queue.get()
                try:
                    self.insert(chunk)
                except sql.exc.SQLAlchemyError:
                    self._insert_with_error(chunk)
                finally:
                    self.log.set(records_written=self.inserted)
                    self.step.queue.task_done()
            if self.event.is_set() is True: break
        pass

    def create(self):
        columns = self._parse_columns()
        primary_key = self._parse_primary_key()
        foreign_keys = self._parse_foreign_keys()
        params = self._parse_params()

        if self.proxy is None:
            self.proxy = sql.Table(self.name, self.db.metadata,
                                   *columns, primary_key, *foreign_keys,
                                   **params, schema=self.schema)
            self.proxy.create(self.db.engine, checkfirst=True)
            self.log.debug(f'Table {self} created')
        else:
            self.log.debug(f'Table {self} already created')
        pass

    def get(self):
        columns = self._parse_columns()
        primary_key = self._parse_primary_key()
        foreign_keys = self._parse_foreign_keys()
        params = self._parse_params()

        if self.proxy is None:
            self.proxy = sql.Table(self.name, self.db.metadata,
                                   *columns, primary_key, *foreign_keys,
                                   **params, schema=self.schema,
                                   autoload=True, autoload_with=self.db.engine)
            self.log.debug(f'Table {self} loaded')
        else:
            self.log.debug(f'Table {self} already loaded')
        pass

    def select(self):
        table = self.name
        table = table if self.schema is None else f'{self.schema}.{table}'
        table = table if self.dblink is None else f'{table}@{self.dblink}'
        query = sql.text(f'SELECT * FROM {table}')
        self.log.info(f'Running SQL query {self.name}...\n\n{query}\n')
        self.answerset = self.db.connection.execute(query)
        self.log.info(f'SQL query {self.name} completed')
        return self.answerset

    def delete(self):
        query = self.proxy.delete()
        answerset = self.db.connection.execute(query)
        self.log.info(f'{answerset.rowcount} records deleted')
        pass

    def drop(self):
        self.proxy.drop(self.db.engine)
        pass

    def insert(self, chunk):
        query = self.proxy.insert()
        self.db.connection.execute(query, chunk)
        self.inserted += len(chunk)
        self.log.info(f'{self.inserted} records inserted')
        pass

    def merge(self, source, keys, updates, inserts=None):
        query = merge(self.proxy).\
            using(source, *keys).\
            update(**updates).\
            insert(**inserts)
        query = query.compile(bind=self.db.engine)
        self.log.info(f'Running SQL query {self.name}...\n\n{query}\n')
        self.answerset = self.db.connection.execute(query)
        self.log.info(f'SQL query {self.name} completed')
        self.merged = self.answerset.rowcount
        self.log.info(f'{self.merged} records merged')
        return self.answerset

    def insert_select(self, select):
        columns = [str(column).replace('"', '') for column in select.columns]
        query = self.proxy.insert()
        if self.append is True:
            query = query.prefix_with('/*+ APPEND */')
        query = query.from_select(columns, select)
        self.log.info(f'Running SQL query {self.name}...\n\n{query}\n')
        self.answerset = self.db.connection.execute(query)
        self.log.info(f'SQL query {self.name} completed')
        self.inserted = self.answerset.rowcount
        self.log.info(f'{self.inserted} records inserted')
        return self.answerset

    def _parse_columns(self):
        self.log.debug(f'Parsing columns of {self}...')
        for column in self.columns:
            name = column['name']
            self.log.debug(f'Parsing column {self.name}.{name}...')

            table = column.get('table', self.name)
            schema = column.get('schema', self.schema)
            dblink = column.get('dblink', self.dblink)

            datatype = column.get('type')
            length = column.get('length')
            precision = column.get('precision')
            scale = column.get('scale')

            colargs = []
            colkwargs = {}

            if datatype is not None:
                if isinstance(column.get('source'), str) is True:
                    source = column['source']
                    datatype = self._inspect_column_datatype(
                        source, datatype, table, schema, dblink)
                    length = self._inspect_column_length(
                        source, length, table, schema, dblink)
                    precision = self._inspect_column_precision(
                        source, precision, table, schema, dblink)
                    scale = self._inspect_column_scale(
                        source, scale, table, schema, dblink)
                else:
                    datatype = self._map_datatype(datatype)

                description = self._compile_column_description(
                    datatype, length, precision, scale)
                colargs.append(description)

            if column.get('sequence') is not None:
                sequence = column.get('sequence')
                if isinstance(sequence, str) is True:
                    sequence = sql.Sequence(sequence)
                elif sequence is True:
                    sequence = f'{self.name}_seq'
                    sequence = sql.Sequence(sequence)
                colargs.append(sequence)

            if column.get('foreign_key') is not None:
                foreign_key = column['foreign_key']
                foreign_key = sql.ForeignKey(foreign_key)
                colargs.append(foreign_key)

            attributes = [
                'autoincrement', 'default', 'index', 'nullable',
                'primary_key', 'unique', 'comment']
            for attribute in attributes:
                if column.get(attribute) is not None:
                    colkwargs[attribute] = column[attribute]

            yield sql.Column(name, *colargs, **colkwargs)

    def _parse_primary_key(self):
        if isinstance(self.primary_key, list) is True:
            return sql.PrimaryKeyConstraint(*self.primary_key)
        elif isinstance(self.primary_key, dict) is True:
            name = self.primary_key.get('name')
            keys = self.primary_key.get('keys', [])
            return sql.PrimaryKeyConstraint(*keys, name=name)

    def _parse_foreign_keys(self):
        for key in self.foreign_keys:
            name = key.get('name')
            table = key.get('table')
            column = key.get('column')
            refcolumn = key.get('refcolumn', column)
            columns = key.get('columns')
            refcolumns = key.get('refcolumns', columns)

            if isinstance(column, str) is True:
                if isinstance(refcolumn, str) is True:
                    refcolumn = f'{table}.{refcolumn}'
                    yield sql.ForeignKeyConstraint(
                        [column], [refcolumn], name=name)
            elif isinstance(columns, list) is True:
                if isinstance(refcolumns, list) is True:
                    refcolumns = [
                        f'{table}.{refcolumn}'
                        for refcolumn in refcolumns]
                    yield sql.ForeignKeyConstraint(
                        columns, refcolumns, name=name)

    def _parse_params(self):
        params = {}
        vendor = self.db.vendor
        if vendor == 'oracle':
            params['oracle_compress'] = self.compress
        return params

    def _map_datatype(self, datatype):
        datatype = datatype.upper()
        if datatype in ['NUMBER', 'DECIMAL', 'NUMERIC']:
            datatype = 'Numeric'
        elif datatype in ['INTEGER', 'INT']:
            datatype = 'Integer'
        elif datatype in ['FLOAT', 'REAL', 'DOUBLE']:
            datatype = 'Float'
        elif datatype in ['STRING', 'VARCHAR', 'VARCHAR2', 'NVARCHAR']:
            datatype = 'String'
        elif datatype in ['CLOB', 'TEXT']:
            datatype = 'Text'
        elif datatype in ['DATE']:
            datatype = 'Date'
        elif datatype in ['DATETIME', 'TIMESTAMP']:
            datatype = 'DateTime'
        elif datatype in ['TIME']:
            datatype = 'Time'
        elif datatype in ['BOOL', 'BOOLEAN']:
            datatype = 'Boolean'

        datatype = getattr(sql, datatype)
        return datatype

    def _inspect_column_datatype(
        self, name, datatype=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and datatype is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_type'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': f"'{name.upper()}'"}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': f"'{schema.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': f"'{table.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                datatype = db.connection.execute(select).scalar()

        # Choose correct SQLAlchemy datatype class name.
        datatype = self._map_datatype(datatype)
        return datatype

    def _inspect_column_length(
        self, name, length=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and length is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_length'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': f"'{name.upper()}'"}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': f"'{schema.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': f"'{table.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)

                    select = Table(**config)._parse_query()
                    length = db.connection.execute(select).scalar()

        return length

    def _inspect_column_precision(
        self, name, precision=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and precision is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_precision'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': f"'{name.upper()}'"}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': f"'{schema.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': f"'{table.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                precision = db.connection.execute(select).scalar()

        return precision

    def _inspect_column_scale(
        self, name, scale=None, table=None, schema=None, dblink=None
    ):
        db = self.db
        if name is not None and scale is None:
            if db.vendor == 'oracle':
                config = {
                    'name': 'all_tab_columns',
                    'dblink': dblink,
                    'select_all': False,
                    'columns': [
                        {'name': 'data_scale'},
                        {
                            'name': 'column_name',
                            'filters': [{'value': f"'{name.upper()}'"}],
                            'select': False
                        }
                    ]
                }

                if isinstance(schema, str) is True:
                    column = {
                        'name': 'owner',
                        'filters': [{'value': f"'{schema.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)
                if isinstance(table, str) is True:
                    column = {
                        'name': 'table_name',
                        'filters': [{'value': f"'{table.upper()}'"}],
                        'select': False}
                    config['columns'].append(column)

                select = Table(**config)._parse_query()
                scale = db.connection.execute(select).scalar()

        return scale

    def _compile_column_description(self, datatype, length, precision, scale):
        if datatype is sql.Numeric:
            return datatype(precision, scale)
        elif datatype is sql.Integer:
            return datatype
        elif datatype is sql.Float:
            return datatype(precision)
        elif datatype is sql.String:
            return datatype(length)
        elif datatype is (sql.Text):
            return datatype(length)
        elif datatype is sql.Date:
            return datatype
        elif datatype is sql.DateTime:
            return datatype
        elif datatype is sql.Time:
            return datatype
        elif datatype is sql.Boolean:
            return datatype

    def _insert_with_error(self, chunk):
        query = self.proxy.insert()
        for row in chunk:
            try:
                current_query = query.values(**row)
                self.db.connection.execute(current_query)
            except sql.exc.SQLAlchemyError:
                self.log.error()
            else:
                self.inserted += 1
        self.log.info(f'{self.inserted} records inserted.')
        pass

class Insert(Table):
    def loader(self):
        if isinstance(self.step.input, (Select, Table)) is True:
            select = self.step.queue.get()
            try:
                self.insert_select(select)
                self.log.set(records_read=self.inserted)
                self.log.set(records_written=self.inserted)
            except:
                self.log.error()
            finally:
                self.step.queue.task_done()
        pass

class Merge(Table):
    def __init__(self, *args, keys=None, updates=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.keys = None
        self.updates = None
        self.configure(keys=keys, updates=updates)
        pass

    def configure(self, *args, keys=None, updates=None, **kwargs):
        super().configure(*args, **kwargs)
        if isinstance(keys, list) is True: self.keys = keys
        if isinstance(updates, list) is True: self.updates = updates
        pass

    def loader(self):
        if isinstance(self.step.input, (Select, Table)) is True:
            select = self.step.queue.get()
            try:
                keys = []
                for key in self.keys:
                    source = key['source']
                    target = key['target']
                    keys.append(select.c[source]==self.proxy.c[target])

                updates = {}
                for update in self.updates:
                    source = update['source']
                    target = update['target']
                    updates[target] = select.c[source]

                inserts = {}
                for column in self.columns:
                    if column.get('insert', True) is True:
                        target = column['name']
                        source = column.get('source', target)
                        inserts[target] = select.c[source]

                self.merge(select, keys, updates, inserts)
                self.log.set(records_read=self.merged,
                             records_written=self.merged,
                             records_updated=self.merged)
            except:
                self.log.error()
            finally:
                self.step.queue.task_done()
        pass

class Select(Item, Extractable, Unloadable, Unexecutable, Fetchable):

    def __init__(self, database=None, statement=None, file=None,
                 distinct=False, all=False, columns=None, tables=None,
                 alias=None, parallel=False, fetch=1000, created=None,
                 title=None, desc=None, bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind, config=config)
        self.database = self.db = None
        self.statement = None
        self.file = None
        self.distinct = None
        self.all = None
        self.columns = []
        self.tables = []
        self.alias = None
        self.parallel = None
        self.fetch = None
        self.created = None
        self.answerset = None
        self.selected = 0

        self.configure(database=database, statement=statement, file=file,
                       distinct=distinct, all=all, columns=columns,
                       tables=tables, alias=alias, parallel=parallel,
                       fetch=fetch, created=created)
        pass

    @property
    def query(self):
        statement = sql.text(self._parse_query())
        return statement

    @property
    def query_with_columns(self):
        columns = self.columns if len(self.columns) > 0 else self.get_columns()
        columns = [
            sql.column(column['name'])
            for column in columns
            if (column.get('name') is not None and
                column.get('select', True) is True)]
        statement = self.query.columns(*columns)
        return statement

    @property
    def query_with_alias(self):
        alias = self.alias or 's'
        statement = self.query_with_columns.alias(name=alias)
        return statement

    def configure(self, database=None, statement=None, file=None,
                  distinct=None, all=None, columns=None, tables=None,
                  alias=None, parallel=None, fetch=None, created=None,
                  config=None):
        if isinstance(config, (str, dict)) is True:
            self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            database = config.get('database', database)
            statement = config.get('statement', statement)
            file = config.get('file', file)
            distinct = config.get('distinct', distinct)
            all = config.get('all', all)
            columns = config.get('columns', columns)
            tables = config.get('tables', tables)
            alias = config.get('alias', alias)
            parallel = config.get('parallel', parallel)
            fetch = config.get('fetch', fetch)
            created = config.get('created', created)

        # Database which stores that table.
        if isinstance(database, Database) is True:
            self.database = self.db = database
        elif isinstance(database, str) is True:
            self.database = self.db = make_database(database)

        if isinstance(statement, str) is True:
            self.statement = statement
        elif isinstance(statement, list) is True:
            self.statement = '\n'.join(statement)
        elif isinstance(file, str) is True:
            self.file = os.path.abspath(file)
            if os.path.exists(self.file) is True:
                self.statement = open(self.file, 'r').read()

        if isinstance(distinct, bool) is True: self.distinct = distinct
        if isinstance(all, bool) is True: self.all = all
        if isinstance(columns, list) is True: self.columns = columns
        if isinstance(tables, list) is True: self.tables = tables
        if isinstance(alias, str) is True: self.alias = alias
        if isinstance(parallel, (bool, int)) is True: self.parallel = parallel
        if isinstance(fetch, int) is True: self.fetch = fetch
        if isinstance(created, dict) is True: self.created = created
        pass

    def extractor(self):
        if isinstance(self.step.output, Insert) is True:
            query = self.query_with_columns
            self.step.queue.put(query)
        elif isinstance(self.step.output, Merge) is True:
            query = self.query_with_alias
            self.step.queue.put(query)
        else:
            self.execute()
            self.log.info('Fetching records...')
            while True:
                chunk = self.fetchmany()
                self.log.set(records_read=self.selected)
                if chunk:
                    self.step.queue.put(chunk)
                else:
                    break
        pass

    def execute(self):
        query = self._parse_query()
        self.log.info(f'Running SQL query...\n\n{query}\n')
        self.answerset = self.db.connection.execute(query)
        self.answerset = self.db.connection.execute(query)
        self.log.info(f'SQL query completed')
        return self.answerset

    def select_count(self):
        query = self._parse_query_count()
        self.log.debug(f'Executing...\n{query}')
        self.answerset = self.db.connection.execute(query).scalar()
        self.counted = self.answerset
        return self.answerset

    def get_columns(self):
        query = self._parse_query()
        query = f'SELECT * FROM ({query}) WHERE 1 = 0'
        result = self.db.connection.execute(query)
        columns = [{'name': key} for key in result.keys()]
        return columns

    def _parse_query(self):
        if self.statement is None:
            query = []

            select = self._parse_select()
            from_ = self._parse_from()
            where = self._parse_where()

            for statement in [select, from_, where]:
                if len(statement) > 0:
                    query.append(statement)

            query = '\n'.join(query)
        else:
            query = self.statement

        query = query.format(pipeline=self._bind, sysinfo=self.log.sysinfo)
        return query

    def _parse_query_count(self):
        query = []

        select = self._parse_select_count()
        from_ = self._parse_from()
        join = self._parse_join()
        where = self._parse_where()

        for statement in [select, from_, join, where]:
            if len(statement) > 0:
                query.append(statement)

        query = '\n'.join(query)
        query = query.format(pipeline=self._bind, sysinfo=self.log.sysinfo)
        return query

    def _parse_select(self):
        t1 = self.tables[0].get('alias') or self.tables[0]['name']

        statement = [r'SELECT']
        # Enable parallel execution for Oracle.
        if hasattr(self.db, 'vendor') is True:
            if self.db.vendor == 'oracle':
                hint = self._parse_oracle_parallel()
                if hint is not None:
                    statement.append(hint)

        # Add DISTINCT keyword if required,
        if self.distinct is True:
            statement.append('DISTINCT')

        # Select all original fields or define the list of necessary fields.
        if self.all is True:
            statement.append(f'*')
        elif self.all is False:
            fields = []
            for column in self.columns:
                # Skip new fields because they are not in input.
                select = column.get('select', True)
                tuple_ = column.get('tuple')
                sql = column.get('sql')
                if isinstance(sql, str) is True:
                    fields.append(sql)
                elif select is True and tuple_ is None:
                    name = column['name']
                    table = column.get('table', t1)
                    value = column.get('value')
                    field = value or f'{table}.{name}'.lower()

                    trim = column.get('trim', False)
                    to_char = column.get('to_char', False)
                    if trim is True or to_char is True or value is not None:
                        if trim is True:
                            field = f'TRIM({field})'
                        if to_char is True:
                            field = f'TO_CHAR({field})'
                        field = f'{field} AS {name}'

                    fields.append(field)
            fields = ', '.join(fields)
            statement.append(fields)
        statement = ' '.join(statement)
        return statement

    def _parse_select_count(self):
        statement = [r'SELECT']
        if hasattr(self.db, 'vendor') is True:
            if self.db.vendor == 'oracle':
                hint = self._parse_oracle_parallel()
                if hint is not None:
                    statement.append(hint)
        statement.append('COUNT(*)')
        statement = ' '.join(statement)
        return statement

    def _parse_from(self):
        statements = []

        for table in self.tables:
            statement = []

            name = table['name']
            schema = table.get('schema')
            dblink = table.get('dblink')
            alias = table.get('alias')

            identity = name
            identity = identity if schema is None else f'{schema}.{identity}'
            identity = identity if dblink is None else f'{identity}@{dblink}'

            if len(statements) == 0:
                statement.extend(['FROM', identity])
                if alias is not None:
                    statement.append(alias.lower())
                t1 = alias or name
                statement = ' '.join(statement)
                statements.append(statement)
            else:
                join = table.get('join', 'inner').upper()
                statement.extend([join, 'JOIN', identity.lower()])
                if alias is not None:
                    statement.append(alias.lower())
                statement = ' '.join(statement)
                statements.append(statement)

                key = table.get('key')
                keys = table.get('keys')
                t2 = (alias or name).lower()
                if isinstance(key, str) is True:
                    key = key.lower()
                    rule = f'ON {t1}.{key} = {t2}.{key}'
                    statements.append(rule)
                elif isinstance(keys, list) is True:
                    for i, key in enumerate(keys):
                        name = key['name']
                        tb = key.get('table', t1)
                        col = key.get('column', name)
                        rel = key.get('relation', '=').upper()

                        if i == 0:
                            rule = f'ON {t2}.{name} {rel} {tb}.{col}'
                            statements.append(rule)
                        else:
                            type = key.get('type', 'and').upper()
                            rule = f'{type} {t2}.{name} {rel} {tb}.{col}'
                            statements.append(rule)

        statements = '\n'.join(statements)
        return statements

    def _parse_where(self):
        statements = []
        t1 = self.tables[0].get('alias') or self.tables[0]['name']
        created = self._parse_created()
        columns = self.columns if created is None else [*self.columns, created]
        for column in columns:
            name = column.get('name')
            tuple_ = column.get('tuple')
            filters = column.get('filters')

            if isinstance(tuple_, list) is True:
                fields = []
                for item in tuple_:
                    name = item['name']
                    table = item.get('table', t1)
                    fields.append(f'{table}.{name}')
                field = ', '.join(fields)
                field = f'({field})'
            elif isinstance(name, str) is True:
                name = column['name']
                table = column.get('table', t1)
                field = f'{table}.{name}'

            if isinstance(filters, list) is True:
                for filter in filters:
                    statement = []

                    type = filter.get('type', 'and').upper()
                    negative = filter.get('negative', False)
                    operator = filter.get('operator', '=').upper()
                    value = filter.get('value')

                    # If it is a first line then start with WHERE.
                    if len(statements) == 0:
                        statement.append('WHERE')
                    else:
                        statement.append(type)

                    # Negative filter must include NOT keyword.
                    if negative is True:
                        statement.append('NOT')

                    statement.append(field)

                    # Value NULL used with IS keyword.
                    if value is None:
                        operator = 'IS'
                    # Tuples and subqueries used with IN keyword.
                    elif isinstance(value, (list, dict)) is True \
                    and operator != 'BETWEEN':
                        operator = 'IN'

                    statement.append(operator)

                    # Process value according to its type.
                    if isinstance(value, (str, int, float)) is True \
                    or value is None:
                        value = self._map_value(value)
                        statement.append(value)
                    elif isinstance(value, list) is True:
                        value = list(map(self._map_value, value))
                        if operator == 'BETWEEN' and len(value) == 2:
                            statement.append(f'{value[0]} AND {value[1]}')
                        else:
                            value = ', '.join(value)
                            value = f'({value})'
                            statement.append(value)
                    elif isinstance(value, dict) is True:
                        value = Select(config=value)
                        value = value.query
                        value = f'(\n{value}\n)'
                        statement.append(value)

                    statement = ' '.join(statement)
                    statements.append(statement)

        statements = '\n'.join(statements)
        return statements

    def _parse_created(self):
        if isinstance(self.created, dict) is True:
            attribute = self.created.get('attribute')
            period = self.created.get('period')
            utc = self.created.get('utc', False)
            history = self.created.get('history')
            start = self.created.get('start')
            end = self.created.get('end')
            today = Today() if self.bind is None else self.bind.today

            if period == 'today':
                period = today
            elif period == 'yesterday':
                period = today.yesterday
            elif period == 'hour-current':
                period = today.hour
            elif period == 'hour-before':
                period = today.hour.before
            elif period == 'month-current':
                period = today.month
            elif period == 'month-before':
                period = today.month.before
            elif period == 'year-current':
                period = today.year
            elif period == 'year-before':
                period = today.year.before

            if start is not None: start = dt.datetime.fromisoformat(start)
            if end is not None: end = dt.datetime.fromisoformat(end)

            start = start or period.start
            end = end or period.end

            if utc is True:
                start = start.astimezone(tz=dt.timezone.utc)
                end = end.astimezone(tz=dt.timezone.utc)

            start = start.strftime('%Y-%m-%d %H:%M:%S')
            end = end.strftime('%Y-%m-%d %H:%M:%S')
            dateformat = 'YYYY-MM-DD HH24:MI:SS'
            start = f"TO_DATE('{start}', '{dateformat}')"
            end = f"TO_DATE('{end}', '{dateformat}')"
            value = [start, end]

            column = {
                'name': attribute['name'],
                'table': attribute.get('table'),
                'select': False,
                'filters': [{'operator': 'between', 'value': value}]
            }

            if isinstance(history, list) is True:
                column = {
                    'tuple': history,
                    'select': False,
                    'filters': [{
                        'value': {
                            'distinct': True,
                            'columns': [*history, column],
                            'tables': self.tables
                        }
                    }]
                }

            return column

    def _map_value(self, value):
        if value is None:
            return 'NULL'
        else:
            return str(value)

    def _parse_oracle_parallel(self):
        if self.parallel is True:
            return '/*+ PARALLEL(auto) */'
        elif self.parallel is False:
            pass
        elif isinstance(self.parallel, int)  is True:
            return f'/*+ PARALLEL({self.parallel}) */'

class TeradataTable(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class CsvFile(Item, Extractable, Loadable, Unexecutable):

    def __init__(self, host=None, name=None, path=None, columns=None,
                 delimiter=';', enclosure=None, endline='\r\n', trim=False,
                 head=True, fetch=1000, purge=True, title=None, desc=None,
                 bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind, config=config)
        self.host = None
        self.name = None
        self.path = None
        self.columns = []
        self.fieldnames = None
        self.delimiter = None
        self.enclosure = None
        self.endline = None
        self.trim = None
        self.head = None
        self.fetch = None
        self.purge = None
        self.dataset = []
        self.read = 0
        self.written = 0

        self.configure(host=host, name=name, path=path, columns=columns,
                       delimiter=delimiter, enclosure=enclosure,
                       endline=endline, trim=trim, head=head, fetch=fetch,
                       purge=purge)
        pass

    def configure(self, host=None, name=None, path=None, columns=None,
                  delimiter=None, enclosure=None, endline=None, trim=None,
                  head=None, fetch=None, purge=None, config=None):
        if isinstance(config, (str, dict)) is True:
            self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            host = config.get('host', host)
            name = config.get('name', name)
            path = config.get('path', path)
            columns = config.get('columns', columns)
            fetch = config.get('fetch', fetch)
            delimiter = config.get('delimiter', delimiter)
            enclosure = config.get('enclosure', enclosure)

        if host is not None:
            self.host = host

        if isinstance(name, str) is True: self.name = name
        if isinstance(path, str) is True: self.path = os.path.abspath(path)
        if isinstance(columns, list) is True:
            self.columns = columns
            self.fieldnames = [column['name'] for column in self.columns]
        if isinstance(delimiter, str) is True: self.delimiter = delimiter
        if isinstance(enclosure, str) is True: self.enclosure = enclosure
        if isinstance(endline, str) is True: self.endline = endline
        if isinstance(trim, bool) is True: self.trim = trim
        if isinstance(head, bool) is True: self.head = head
        if isinstance(fetch, int) is True: self.fetch = fetch
        if isinstance(purge, bool) is True: self.purge = purge
        pass

    def prepare(self):
        if self.purge is True:
            open(self.path, 'w+').close()
            self.log.info(f'all records deleted')
        pass

    def extractor(self):
        dataset = self.read_records()
        length = len(self.read_records())
        i = 0
        while i < length:
            chunk = dataset[0:self.fetch]
            del dataset[0:self.fetch]
            read = len(chunk)
            i += read
            self.read += read
            self.log.info(f'{self.read} records read')
            self.log.set(records_read=self.read, files_read=1,
                         bytes_read=os.stat(self.path).st_size)
            self.step.queue.put(chunk)
        pass

    def loader(self):
        while True:
            if self.step.queue.empty() is False:
                chunk = self.step.queue.get()
                try:
                    self.write_records(chunk)
                except:
                    self.log.error()
                finally:
                    self.log.set(records_written=self.written, files_written=1,
                                 bytes_written=os.stat(self.path).st_size)
                    self.step.queue.task_done()
            if self.event.is_set() is True: break
        pass

    def read_records(self):
        with open(self.path, 'r') as fh:
            dialect = self._parse_dialect()
            fieldnames = self.fieldnames
            reader = csv.DictReader(fh, fieldnames, **dialect)
            self.dataset = [dict(row) for row in reader]
        return self.dataset

    def write_records(self, chunk):
        chunk = list([dict(record) for record in chunk])
        with open(self.path, 'a+', newline='') as fh:
            dialect = self._parse_dialect()
            if self.head is True:
                if (self.fieldnames is not None or
                    isinstance(chunk[0], dict) is True):
                    fieldnames = self.fieldnames or list(chunk[0].keys())
                    writer = csv.DictWriter(fh, fieldnames, **dialect)
                    if len(fh.readlines()) == 0:
                        writer.writeheader()
                    writer.writerows(chunk)
                else:
                    raise ValueError('header can not be defined')
            elif self.head is False:
                writer = csv.writer(fh, **dialect)
                writer.writerows([row.values() for row in chunk])
            self.written += len(chunk)
            self.log.info(f'{self.written} records written')
        pass

    def _parse_dialect(self):
        delimiter = self.delimiter
        quotechar = self.enclosure
        quoting = csv.QUOTE_NONE if quotechar is None else csv.QUOTE_ALL
        lineterminator = self.endline
        skipinitialspace = self.trim

        dialect = {'delimiter': delimiter, 'quotechar': quotechar,
                   'quoting': quoting, 'lineterminator': lineterminator,
                   'skipinitialspace': skipinitialspace}
        return dialect

class JsonFile(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class XmlFile(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class Asn1File(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class SqlScript(Item, Unextractable, Unloadable, Executable):

    def __init__(self, database=None, statement=None, file=None,
                 title=None, desc=None, bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind)
        self.database = None
        self.statements = None
        self.file = None
        self.executed = 0

        self.configure(database=database, statement=statement, file=file,
                       config=config)
        pass

    def configure(self, database=None, statement=None, file=None,
                  config=None):
        if isinstance(config, (str, dict)) is True:
            self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            database = config.get('database', database)
            statement = config.get('statement', statement)
            file = config.get('file', file)

        if isinstance(database, Database) is True:
            self.database = self.db = database
        elif isinstance(database, str) is True:
            self.database = self.db = make_database(database)

        if isinstance(statement, str) is True:
            pass
        elif isinstance(statement, list) is True:
            statement = '\n'.join(statement)
        elif isinstance(file, str) is True:
            self.file = os.path.abspath(file)
            if os.path.exists(self.file) is True:
                statement = open(self.file, 'r').read()
        self.statements = self._split_statements(statement)
        pass

    def execute(self):
        for statement in self.statements:
            query = statement.format(pipeline=self._bind,
                                     sysinfo=self.log.sysinfo)
            self.log.info(f'Running SQL query...\n\n{query}\n')
            self.database.connection.execute(query)
            self.executed += 1
        pass

    def _split_statements(self, text):
        statements = [statement for statement in text.split(';')
                      if len(statement) > 1]
        return list(statements)

class TeradataScript(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class TeradataFastload(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class BashScript(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class Filenames(Item, Extractable, Unloadable, Unexecutable):

    def __init__(self, host=None, path=None, recursive=False, mask=None,
                 created=None, title=None, desc=None, bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind, config=config)
        self.host = None
        self.path = None
        self.mask = None
        self.created = None
        self.recursive = None
        self.files_found = 0
        self.bytes_found = 0
        self.files_matched = 0
        self.bytes_matched = 0

        self.configure(host=host, path=path, recursive=recursive,
                       mask=mask, created=created)
        pass

    def configure(self, host=None, path=None, recursive=None, mask=None,
                  created=None, config=None):
        if config is not None: self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            host = config.get('host', host)
            path = config.get('path', path)
            recursive = config.get('recursive', recursive)
            mask = config.get('mask', mask)
            created = config.get('created', created)

        if isinstance(host, (Remote, Localhost)) is True:
            self.host = host
        elif isinstance(host, str) is True:
            self.host = Localhost() if host == 'localhost' else make_remote(host)

        if isinstance(path, str) is True: self.path = path
        if isinstance(recursive, bool) is True: self.recursive = recursive
        if isinstance(mask, str) is True: self.mask = mask
        if isinstance(created, (dict, str)) is True: self.created = created
        pass

    def extractor(self):
        files = list(self.get_filtered())
        self.step.queue.put(files)
        self.log.info(f'{self.files_found} files found')
        self.log.info(f'{self.files_matched} files matched')
        self.log.set(files_read=self.files_matched,
                     bytes_read=self.bytes_matched)
        pass

    def get_all(self):
        for row in self._process_files():
            path = row['path']
            self.log.debug(f'{path} found')
            yield row

    def get_filtered(self):
        date_from, date_to = self._map_created_dates()
        self.log.debug(f'date_from is <{date_from}>')
        self.log.debug(f'date_to is <{date_to}>')
        for row in self._process_files():
            path = row['path']
            mtime = row['mtime']
            size = row['size']
            if self.mask is not None:
                if re.match(self.mask, os.path.basename(path)) is None:
                    self.log.debug(f'{path} filtered by mask')
                    continue
            if mtime < date_from or mtime > date_to:
                self.log.debug(f'{path} filtered by date')
                continue
            self.log.debug(f'{path} matched')
            self.files_matched += 1
            self.bytes_matched += size
            yield row

    def _process_files(self, dir=None):
        dir = dir or self.path
        if isinstance(self.host, Localhost) is True:
            dir = os.path.normpath(dir)
            if os.path.exists(dir) is True:
                for filename in os.listdir(dir):
                    path = os.path.join(dir, filename)
                    stats = os.stat(path)
                    isdir, isfile, mtime, size = self._parse_file_stats(stats)
                    relpath = os.path.relpath(dir, self.path)
                    row = dict(host=self.host, path=path,
                               root=os.path.normpath(self.path), dir=relpath,
                               filename=filename, isdir=isdir, isfile=isfile,
                               mtime=mtime, size=size)
                    if isdir is True and self.recursive is True:
                        yield from self._process_files(dir=path)
                    elif isfile is True or isdir is True:
                        self.files_found += 1
                        self.bytes_found += size
                        yield row
        elif self.host.is_sftp is True:
            if self.host.exists(dir) is True:
                for stats in self.host.sftp.listdir_attr(dir):
                    path = self.host.sftp.normalize(f'{dir}/{stats.filename}')
                    isdir, isfile, mtime, size = self._parse_file_stats(stats)
                    relpath = os.path.relpath(dir, self.path)
                    row = dict(host=self.host, path=path, root=self.path,
                               dir=relpath, filename=stats.filename,
                               isdir=isdir, isfile=isfile, mtime=mtime,
                               size=size)
                    if isdir is True and self.recursive is True:
                        yield from self._process_files(dir=path)
                    elif isfile is True or isdir is True:
                        self.files_found += 1
                        self.bytes_found += size
                        yield row
        # NOT FINISHED YET AND NOT RECOMMENDED TO USE WHEN ROOT HAVE FOLDERS
        # FTP support is pretty poor. So here not all FTP server will return
        # modification time and script will certainly fail in case of directory
        # found.
        elif self.host.is_ftp is True:
            if self.host.exists(dir) is True:
                for path in self.host.ftp.nlst():
                    filename = os.path.basename(path)
                    relpath = '.'
                    isdir = False
                    isfile = True
                    mtime = self.host.ftp.sendcmd(f'MDTM {path}')[4:]
                    mtime = dt.datetime.strptime(mtime, '%Y%m%d%H%M%S')
                    size = self.host.ftp.size(path)
                    row = dict(host=self.host, path=path, root=self.path,
                               filename=filename, dir=relpath, isdir=isdir,
                               isfile=isfile, mtime=mtime, size=size)
                    self.files_found += 1
                    self.bytes_found += size
                    yield row

    def _parse_file_stats(self, stats):
        isdir = stat.S_ISDIR(stats.st_mode)
        isfile = stat.S_ISREG(stats.st_mode)
        mtime = dt.datetime.fromtimestamp(stats.st_mtime)
        size = stats.st_size
        return (isdir, isfile, mtime, size)

    def _map_created_dates(self):
        today = self.bind.today if hasattr(self.bind, 'today') is True else None
        today = today or Today(dt.datetime.now())

        if isinstance(self.created, dict) is True:
            date_from = self.created.get('date_from')
            date_from = None if date_from is None else eval(date_from)
            date_from = date_from or dt.datetime.min

            date_to = self.created.get('date_to')
            date_to = None if date_to is None else eval(date_to)
            date_to = date_to or dt.datetime.max
        elif isinstance(self.created, str) is True:
            date_from = eval(date_from)
            date_to = dt.datetime.max
        else:
            date_from = dt.datetime.min
            date_to = dt.datetime.max
        return (date_from, date_to)

class FileManager(Item, Unextractable, Loadable, Unexecutable):

    def __init__(self, host=None, action='copy', dest=None, nodir=False,
                 zip=False, unzip=False, tempname=False, backup=False,
                 title=None, desc=None, bind=None, config=None):
        super().__init__(title=title, desc=desc, bind=bind, config=config)
        self.host = None
        self.action = None
        self.dest = []
        self.nodir = None
        self.zip = None
        self.unzip = None
        self.tempname = None
        self.backup = None
        self.files_copied = 0
        self.files_deleted = 0
        self.bytes_copied = 0
        self.bytes_deleted = 0
        self.files_error = 0
        self._fileinfos = None

        self.configure(host=host, action=action, dest=dest, nodir=nodir,
                       zip=zip, unzip=unzip, tempname=tempname, backup=backup)
        pass

    @property
    def fileinfos(self):
        return self._fileinfos

    @fileinfos.setter
    def fileinfos(self, value):
        if isinstance(value, list) is True or value is None:
            self._fileinfos = value
        else:
            raise TypeError('fileinfo must be list '
                            f'not {value.__class__.__name__}')

    def configure(self, host=None, action=None, dest=None, nodir=None,
                  zip=None, unzip=None, tempname=None, backup=None,
                  config=None):
        if config is not None: self.config = config
        if self.config is not None:
            config = make_config(obj=self.config)
            host = config.get('host', host)

        if isinstance(host, (Remote, Localhost)) is True:
            self.host = host
        elif host == 'localhost':
            self.host = Localhost()
        elif isinstance(host, str) is True:
            self.host = make_remote(host)

        if (isinstance(action, str) is True and
            action.lower() in ('copy', 'move', 'delete', 'rename')):
            self.action = action.lower()

        if isinstance(dest, list) is True: self.dest = dest
        if isinstance(dest, str) is True: self.dest = [dest]
        if isinstance(nodir, bool) is True: self.nodir = nodir
        if isinstance(zip, bool) is True: self.zip = zip
        if isinstance(unzip, bool) is True: self.unzip = unzip
        if isinstance(tempname, bool) is True: self.tempname = tempname
        if isinstance(backup, (str, bool)) is True: self.backup = backup
        pass

    def loader(self):
        self.fileinfos = self.step.queue.get()
        self.log.info('Starting file processing...')
        for fileinfo in self.fileinfos:
            self.perform(fileinfo)
        self.log.info('File processing finished')
        self.log.info(f'{self.files_copied} files copied')
        self.log.info(f'{self.files_deleted} files deleted')
        self.log.info(f'{self.files_error} files with error')
        self.log.set(files_written=self.files_copied,
                     bytes_written=self.bytes_copied,
                     errors=self.files_error)
        self.step.queue.task_done()
        pass

    def perform(self, fileinfo):
        host = fileinfo['host']
        path = fileinfo['path']
        size = fileinfo['size']
        self.log.debug(f'Processing {path}')
        try:
            # Source and target are local.
            if (isinstance(self.host, Localhost) and
                isinstance(host, Localhost) is True):
                self.log.debug('On localhost by OS')
                if self.action in ('copy', 'move'):
                    self._copy_on_localhost(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source and target are the same remote, SSH enabled.
            elif (host == self.host and
                  host.is_ssh is True):
                  self.log.debug('On remote by SSH')
                  if self.action in ('copy', 'move'):
                      self._copy_on_remote_by_ssh(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_ssh(fileinfo)
            # Source and target are the same remote, SFTP enabled.
            elif (host == self.host and
                  host.is_sftp is True):
                  self.log.debug('On remote by SFTP and OS')
                  if self.action in ('copy', 'move'):
                      self._copy_on_remote_by_sftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are the same remote, FTP enabled.
            elif (host == self.host and
                  host.is_ftp is True):
                  self.log.debug('On remote by FTP and OS')
                  if self.action in ('copy', 'move'):
                      self._copy_on_remote_by_ftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_ftp(fileinfo)
            # Source is local, target is remote, SFTP enabled.
            elif (host != self.host and
                  isinstance(host, Localhost) is True and
                  isinstance(self.host, Remote) is True and
                  self.host.is_sftp is True):
                  self.log.debug('To remote by SFTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_localhost_to_remote_by_sftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, SFTP enabled.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Localhost) is True and
                  host.is_sftp is True):
                  self.log.debug('To localhost by SFTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_to_localhost_by_sftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_sftp(fileinfo)
            # Source is local, target is remote, FTP enabled.
            elif (host != self.host and
                  isinstance(host, Localhost) is True and
                  isinstance(self.host, Remote) is True and
                  self.host.is_ftp is True):
                  self.log.debug('To remote by FTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_localhost_to_remote_by_ftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, FTP enabled.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Localhost) is True and
                  host.is_ftp is True):
                  self.log.debug('To localhost by FTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_to_localhost_by_ftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP on both enabled.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Remote) is True and
                  host.is_sftp is True and
                  self.host.is_sftp is True):
                  self.log.debug('From remote by SFTP to remote by SFTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_to_remote_by_sftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are different remotes, FTP on both enabled.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Remote) is True and
                  host.is_ftp is True and
                  self.host.is_ftp is True):
                  self.log.debug('From remote by FTP to remote by FTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_to_remote_by_ftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, FTP enabled on source,
            # SFTP enabled on target.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Remote) is True and
                  host.is_ftp is True and
                  self.host.is_sftp is True):
                  self.log.debug('From remote by FTP to remote by SFTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_by_ftp_to_remote_by_sftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP enabled on source,
            # FTP enabled on target.
            elif (host != self.host and
                  isinstance(host, Remote) is True and
                  isinstance(self.host, Remote) is True and
                  host.is_sftp is True and
                  self.host.is_ftp is True):
                  self.log.debug('From remote by SFTP to remote by FTP')
                  if self.action in ('copy', 'move'):
                      self._copy_from_remote_by_sftp_to_remote_by_ftp(fileinfo)
                  if self.action in ('move', 'delete'):
                      self._delete_on_remote_by_sftp(fileinfo)
        except:
            self.files_error += 1
            self.log.warning()
        else:
            if self.action in ('copy', 'move'):
                self.files_copied += 1
                self.bytes_copied += size
            if self.action in ('move', 'delete'):
                self.files_deleted += 1
                self.bytes_deleted += size
        pass

    def _copy_on_localhost(self, fileinfo):
        for path in self.dest:
            self.log.debug(f'To {path}')
            if fileinfo['isfile'] is True:
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodir is False else '.'
                filename = fileinfo['filename']
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False: os.makedirs(dest)
                dest = os.path.join(dest, filename)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source, 'rb') as fhi:
                        with open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Unzipped {source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with gzip.open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Zipped {source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                shutil.copyfile(source, temp or dest)
                if temp is not None: os.rename(temp, dest)
                self.log.info(f'Copied {source} to {dest}')
        pass

    def _copy_on_remote_by_ssh(self, fileinfo):
        remote = fileinfo['host']
        for path in self.dest:
            if fileinfo['isfile'] is True:
                self.log.debug(f'To {path}')
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodir is False else '.'
                filename = fileinfo['filename']
                dest = f'{path}/{dir}'
                remote.execute(f'mkdir -p {dest}')
                dest = f'{dest}/{filename}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    remote.execute(f'gunzip -c {source} > {temp or dest}')
                    if temp is not None: remote.execute(f'mv {temp} {dest}')
                    self.log.info(f'Unzipped {remote}:{source} '
                                  f'to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    remote.execute(f'gzip -c {source} > {temp or dest}')
                    if temp is not None: remote.execute(f'mv {temp} {dest}')
                    self.log.info(f'Zipped {remote}:{source} '
                                  f'to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                remote.execute(f'cp {source} {temp or dest}')
                if temp is not None: remote.execute(f'mv {temp} {dest}')
                self.log.info(f'Copied {remote}:{source} '
                              f'to {remote}:{dest}')
        pass

    def _copy_on_remote_by_sftp(self, fileinfo):
        self._copy_from_remote_to_remote_by_sftp(fileinfo)
        pass

    def _copy_on_remote_by_ftp(self, fileinfo):
        self._copy_from_remote_to_remote_by_ftp(fileinfo)
        pass

    def _copy_from_remote_to_localhost_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = fileinfo['host']
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False: os.makedirs(dest)
                dest = os.path.join(dest, filename)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        remote.sftp.getfo(source, fhi)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            with open(temp or dest, 'wb') as fho:
                                shutil.copyfileobj(gz, fho)
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Unzipped {remote}:{source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(temp or dest, 'wb') as fho:
                        remote.sftp.getfo(source, fho)
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Zipped {remote}:{source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                remote.sftp.get(source, temp or dest)
                if temp is not None: os.rename(temp, dest)
                self.log.info(f'Copied {remote}:{source} to {dest}')
        pass

    def _copy_from_localhost_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = remote.sftp.normalize(f'{path}/{dir}')
                if remote.exists(dest) is False: remote.sftp.mkdir(dest)
                dest = remote.sftp.normalize(f'{dest}/{filename}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source, 'rb') as fhi:
                        remote.sftp.putfo(fhi, temp or dest)
                    if temp is not None: remote.sftp.rename(temp, dest)
                    self.log.info(f'Unzipped {source} to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with tempfile.TemporaryFile() as fho:
                            with gzip.GzipFile(filename=filename, mode='wb',
                                               fileobj=fho) as gz:
                                shutil.copyfileobj(fhi, gz)
                            fho.seek(0)
                            remote.sftp.putfo(fho, temp or dest)
                    if temp is not None: remote.sftp.rename(temp, dest)
                    self.log.info(f'Zipped {source} to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                remote.sftp.put(source, temp or dest)
                if temp is not None: remote.sftp.rename(temp, dest)
                self.log.info(f'Copied{source} to {remote}:{dest}')
        pass

    def _copy_from_remote_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            host_in = fileinfo['host']
            host_out = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = host_out.sftp.normalize(f'{path}/{dir}')
                if host_out.exists(dest) is False: host_out.sftp.mkdir(dest)
                dest = host_out.sftp.normalize(f'{dest}/{filename}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        host_in.sftp.getfo(source, th)
                        th.seek(0)
                        with gzip.GzipFile(fileobj=th) as gz:
                            host_out.sftp.putfo(gz, temp or dest)
                    if temp is not None: host_out.sftp.rename(temp, dest)
                    self.log.info(f'Unzipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        with gzip.GzipFile(filename=filename, mode='wb',
                                           fileobj=th) as gz:
                            host_in.sftp.getfo(source, gz)
                        th.seek(0)
                        host_out.sftp.putfo(th, temp or dest)
                    if temp is not None: host_out.sftp.rename(temp, dest)
                    self.log.info(f'Zipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    host_in.sftp.getfo(source, th)
                    th.seek(0)
                    host_out.sftp.putfo(th, temp or dest)
                if temp is not None: host_out.sftp.rename(temp, dest)
                self.log.info(f'Copied {host_in}:{source} to {host_out}:{dest}')
        pass

    def _copy_from_localhost_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if remote.exists(dest) is False: remote.ftp.mkd(dest)
                dest = f'{dest}/{filename}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source) as fhi:
                        with tempfile.TemporaryFile() as fho:
                            shutil.copyfileobj(fhi, fho)
                            fho.seek(0)
                            remote.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None: remote.ftp.rename(temp, dest)
                    self.log.info(f'Unzipped {source} to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with tempfile.TemporaryFile() as fho:
                            with gzip.GzipFile(filename=filename, mode='wb',
                                               fileobj=fho) as gz:
                                shutil.copyfileobj(fhi, gz)
                            fho.seek(0)
                            remote.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None: remote.ftp.rename(temp, dest)
                    self.log.info(f'Zipped {source} to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(source, 'rb') as fhi:
                    remote.ftp.storbinary(f'STOR {temp or dest}', fhi)
                if temp is not None: remote.ftp.rename(temp, dest)
                self.log.info(f'Copied {source} to {remote}:{dest}')
        pass

    def _copy_from_remote_to_localhost_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = fileinfo['host']
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False: os.makedirs(dest)
                dest = os.path.join(dest, filename)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        remote.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            with open(temp or dest, 'wb') as fho:
                                shutil.copyfileobj(gz, fho)
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Unzipped {remote}:{source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        remote.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.open(temp or dest, 'wb') as fho:
                            shutil.copyfileobj(fhi, fho)
                    if temp is not None: os.rename(temp, dest)
                    self.log.info(f'Zipped {remote}:{source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(temp or dest, 'wb') as fho:
                    remote.ftp.retrbinary(f'RETR {source}', fho.write)
                if temp is not None: os.rename(temp, dest)
                self.log.info(f'Copied {remote}:{source} to {dest}')
        pass

    def _copy_from_remote_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            host_in = fileinfo['host']
            host_out = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if host_out.exists(dest) is False: host_out.ftp.mkd(dest)
                dest = f'{dest}/{filename}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        host_in.ftp.retrbinary(f'RETR {source}', th.write)
                        th.seek(0)
                        with gzip.GzipFile(fileobj=th) as gz:
                            host_out.ftp.storbinary(f'STOR {temp or dest}', gz)
                    if temp is not None: host_out.ftp.rename(temp, dest)
                    self.log.info(f'Unzipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        with gzip.GzipFile(filename=filename, mode='wb',
                                           fileobj=th) as gz:
                            host_in.ftp.retrbinary(f'RETR {source}', gz.write)
                        th.seek(0)
                        host_out.ftp.storbinary(f'STOR {temp or dest}', th)
                    if temp is not None: host_out.ftp.rename(temp, dest)
                    self.log.info(f'Zipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    host_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    host_out.ftp.storbinary(f'STOR {temp or dest}', th)
                if temp is not None: host_out.ftp.rename(temp, dest)
                self.log.info(f'Copied {host_in}:{source} to {host_out}:{dest}')
        pass

    def _copy_from_remote_by_ftp_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            host_in = fileinfo['host']
            host_out = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = host_out.sftp.normalize(f'{path}/{dir}')
                if host_out.exists(dest) is False: host_out.sftp.mkdir(dest)
                dest = host_out.sftp.normalize(f'{dest}/{filename}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        host_in.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            host_out.sftp.putfo(gz, temp or dest)
                    if temp is not None: host_out.sftp.rename(temp, dest)
                    self.log.info(f'Unzipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        with gzip.GzipFile(filename=filename, mode='wb',
                                           fileobj=fhi) as gz:
                            host_in.ftp.retrbinary(f'RETR {source}', gz.write)
                        fhi.seek(0)
                        host_out.sftp.putfo(fhi, temp or dest)
                    if temp is not None: host_out.sftp.rename(temp, dest)
                    self.log.info(f'Zipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    host_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    host_out.sftp.putfo(th, temp or dest)
                if temp is not None: host_out.sftp.rename(temp, dest)
                self.log.info(f'Copied {host_in}:{source} to {host_out}:{dest}')
        pass

    def _copy_from_remote_by_sftp_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            host_in = fileinfo['host']
            host_out = self.host
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            filename = fileinfo['filename']
            for path in self.dest:
                self.log.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if host_out.exists(dest) is False: host_out.ftp.mkd(dest)
                dest = f'{dest}/{filename}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        host_in.sftp.getfo(source, fhi)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            host_out.ftp.storbinary(f'STOR {temp or dest}', gz)
                    if temp is not None: host_out.ftp.rename(temp, dest)
                    self.log.info(f'Unzipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fho:
                        with gzip.GzipFile(filename=filename, mode='wb',
                                           fileobj=fho) as gz:
                            host_in.sftp.getfo(source, gz)
                        fho.seek(0)
                        host_out.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None: host_out.ftp.rename(temp, dest)
                    self.log.info(f'Zipped {host_in}:{source} to '\
                                  f'{host_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    host_in.sftp.getfo(source, th)
                    th.seek(0)
                    host_out.ftp.storbinary(f'STOR {dest}', th)
                if temp is not None: host_out.ftp.rename(temp, dest)
                self.log.info(f'Copied {host_in}:{source} to {host_out}:{dest}')
        pass

    def _delete_on_localhost(self, fileinfo):
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            os.remove(path)
            self.log.info(f'Deleted {path}')
        pass

    def _delete_on_remote_by_ssh(self, fileinfo):
        remote = fileinfo['host']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.execute(f'rm -f {path}')
            self.log.info(f'Deleted {remote}:{path}')
        pass

    def _delete_on_remote_by_sftp(self, fileinfo):
        remote = fileinfo['host']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.sftp.remove(path)
            self.log.info(f'Deleted {remote}:{path}')
        pass

    def _delete_on_remote_by_ftp(self, fileinfo):
        remote = fileinfo['host']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.ftp.delete(path)
            self.log.info(f'Deleted {remote}:{path}')
        pass

class FileMerge(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class Log(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class Email(Item):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

class DoNothing(Item, Unextractable, Unloadable, Executable):

    def __init__(self, title=None, desc=None, bind=None):
        super().__init__(title=title, desc=desc, bind=bind)
        pass

    def execute(self):
        self.log.info('Nothing to do')
        pass
