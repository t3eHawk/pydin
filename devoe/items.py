"""Contains Item prototypes and built-in Items."""

import csv
import datetime as dt
import gzip
import json
import os
import re
import shutil
import stat
import tempfile
import threading as th
import time as tm
import xml.etree.ElementTree as xet
import xml.dom.minidom as xdom

import sqlalchemy as sa
import sqlparse as spe

from .config import connector, calendar
from .config import Localhost, Server, Database
from .logger import logger

from .utils import to_sql
from .utils import coalesce

from .core import Item


class Extractable():
    """Represents extractable Item."""

    extractable = True

    def to_extractor(self, step, queue):
        """Start Item extractor."""
        name = f'{step.thread.name}-Extractor'
        target = dict(target=self.extractor, args=(step, queue))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def extractor(self, step, queue):
        """Extract data."""
        logger.info(f'Reading {self} records...')
        for dataset in self.extract(step):
            try:
                queue.put(dataset)
                step.records_read = len(dataset)
                logger.info(f'{step.records_read} records read')
            except Exception:
                logger.error()
        pass

    pass


class Transformable():
    """Represents transformable Item."""

    transformable = True

    def to_transformer(self, step, input, output):
        """Start Item transformer."""
        name = f'{step.thread.name}-Transformer'
        target = dict(target=self.transformator, args=(step, input, output))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def transformator(self, step, input, output):
        """Transform data."""
        logger.info(f'Processing {self} records...')
        processed = 0
        while True:
            if input.empty() is True:
                if step.extraction is True:
                    tm.sleep(0.001)
                    continue
                break
            else:
                inputs = input.get()
                try:
                    outputs = list(map(self.transform, inputs))
                except Exception:
                    logger.error()
                else:
                    output.put(outputs)
                    processed += len(outputs)
                    logger.info(f'{processed} records processed')
                    input.task_done()
        pass

    pass


class Loadable():
    """Represents loadable Item."""

    loadable = True

    def to_loader(self, step, queue):
        """Start Item loader."""
        name = f'{step.thread.name}-Loader'
        target = dict(target=self.loader, args=(step, queue))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def loader(self, step, queue):
        """Load data."""
        logger.info(f'Writing {self} records...')
        while True:
            if queue.empty() is True:
                if step.extraction is True or step.transformation is True:
                    tm.sleep(0.001)
                    continue
                break
            else:
                dataset = queue.get()
                try:
                    result = self.load(step, dataset)
                except Exception:
                    logger.error()
                else:
                    step.records_written = len(coalesce(result, dataset))
                    logger.info(f'{step.records_written} records written')
                    queue.task_done()
        pass

    pass


class Executable():
    """Represents executable Item."""

    executable = True

    def to_executor(self, step):
        """Start Item executor."""
        name = f'{step.thread.name}-Executor'
        target = dict(target=self.executor, args=(step,))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def executor(self, step):
        """Execute instructions."""
        try:
            result = self.execute(step)
            if isinstance(result, int):
                setattr(step, 'result_value', result)
            elif isinstance(result, (list, tuple, dict, str)):
                setattr(step, 'result_long', result)
        except Exception:
            logger.error()
        pass

    pass


class DB():
    """Represents base class for all DB items."""

    @property
    def db(self):
        """Get database object (short)."""
        return self._database

    @property
    def database(self):
        """Get database object."""
        return self._database

    @database.setter
    def database(self, value):
        if isinstance(value, Database):
            self._database = value
        elif isinstance(value, str):
            self._database = connector.receive(value)
        pass

    pass


class OS():
    """Represents base class for all OS items."""

    @property
    def server(self):
        """Get server object."""
        return self._server

    @server.setter
    def server(self, value):
        if isinstance(value, (Localhost, Server)):
            self._server = value
        elif isinstance(value, str):
            if value == 'localhost':
                self._server = Localhost()
            else:
                self._server = connector.receive(value)
        pass

    pass


class Mapper(Transformable, Item):
    """Represents basic mapper used for data transformation."""

    def __init__(self, item_name=None):
        super().__init__(name=(item_name or __class__.__name__))
        pass

    def transform(self, input):
        """Transform data."""
        return input

    pass


class Table(Extractable, Loadable, DB, Item):
    """Represents database table as ETL Item."""

    def __init__(self, item_name=None, database=None, schema=None,
                 table_name=None, db_link=None, fetch_size=1000,
                 purge=False, append=False):
        super().__init__(name=(item_name or __class__.__name__))
        self._database = None
        self._schema = None
        self._table_name = None
        self._db_link = None
        self._fetch_size = None
        self._purge = None
        self._append = None

        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.db_link = db_link
        self.fetch_size = fetch_size
        self.purge = purge
        self.append = append

        pass

    @property
    def schema(self):
        """Describe schema name."""
        return self._schema

    @schema.setter
    def schema(self, value):
        if isinstance(value, str):
            self._schema = value.lower()
        pass

    @property
    def table_name(self):
        """Describe schema name."""
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        if isinstance(value, str):
            self._table_name = value.lower()
        pass

    @property
    def db_link(self):
        """Describe DB link name."""
        return self._db_link

    @db_link.setter
    def db_link(self, value):
        if isinstance(value, str):
            self._db_link = value.lower()
        pass

    @property
    def fetch_size(self):
        """Describe fetch size value."""
        return self._fetch_size

    @fetch_size.setter
    def fetch_size(self, value):
        if isinstance(value, int):
            self._fetch_size = value
        pass

    @property
    def purge(self):
        """Describe flag defining whether data purge is needed or not."""
        return self._purge

    @purge.setter
    def purge(self, value):
        if isinstance(value, bool):
            self._purge = value
        pass

    @property
    def append(self):
        """Describe flag defining whether append hint needed or not."""
        return self._append

    @append.setter
    def append(self, value):
        if isinstance(value, bool):
            self._append = value
        pass

    @property
    def exists(self):
        """Check if table exists."""
        if self.db is not None:
            return self.db.engine.has_table(self.table_name)
        pass

    def configure(self, database=None, schema=None, table_name=None,
                  db_link=None, fetch_size=None, append=None, purge=None):
        """Configure the Item properties."""
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.db_link = db_link
        self.fetch_size = fetch_size
        self.purge = purge
        self.append = append
        pass

    def get_table(self):
        """Get object representing table."""
        name = self.table_name
        meta = sa.MetaData()
        engine = self.db.engine
        table = sa.Table(name, meta, schema=self.schema,
                         autoload=True, autoload_with=engine)
        return table

    def get_address(self):
        """Get full database table address (schema, table name, db link)."""
        table = self.table_name
        table = table if self.schema is None else f'{self.schema}.{table}'
        table = table if self.db_link is None else f'{table}@{self.db_link}'
        return table

    def select(self):
        """Select table data."""
        conn = self.db.connect()
        table = self.get_address()
        query = sa.text(to_sql(f'select * from {table}'))
        logger.info(f'Running SQL query <{self.table_name}>...')
        logger.line(f'-------------------\n{query}\n-------------------')
        answerset = conn.execute(query)
        logger.info(f'SQL query <{self.table_name}> completed')
        return answerset

    def fetch(self):
        """Fetch data from the table."""
        answerset = self.select()
        while True:
            dataset = answerset.fetchmany(self.fetch_size)
            if dataset:
                yield [dict(record) for record in dataset]
            else:
                break
        pass

    def insert(self, chunk):
        """Insert data in chunk to the table."""
        conn = self.db.connect()
        table = self.get_table()
        query = table.insert()
        return conn.execute(query, chunk)

    def delete(self):
        """Delete table data."""
        conn = self.db.connect()
        table = self.get_table()
        query = table.delete()
        result = conn.execute(query)
        logger.info(f'{result.rowcount} {self.table_name} records deleted')
        pass

    def truncate(self):
        """Truncate table data."""
        conn = self.db.engine.connect()
        table = self.get_address()
        query = sa.text(f'truncate table {table}')
        conn.execute(query)
        logger.info(f'Table {self.table_name} truncated')
        pass

    def prepare(self):
        """Prepare table."""
        if self.purge is True:
            logger.debug(f'Table {self.table_name} will be purged')
            if self.db.vendor == 'oracle':
                self.truncate()
            else:
                self.delete()
        pass

    def extract(self, step):
        """Extract data."""
        return self.fetch()

    def load(self, step, dataset):
        """Load data."""
        self.insert(dataset)
        pass

    pass


class SQL(Executable, DB, Item):
    """Represents SQL script as ETL Item."""

    def __init__(self, item_name=None, database=None, text=None, file=None,
                 parallel=False):
        super().__init__(name=(item_name or __class__.__name__))
        self.database = database
        self.text = text
        self.file = file
        self.parallel = parallel
        pass

    @property
    def text(self):
        """Get raw SQL text."""
        return self._text

    @text.setter
    def text(self, value):
        if isinstance(value, str):
            self._text = to_sql(value)
        pass

    @property
    def file(self):
        """Get path to file containing select SQL text."""
        return self._file

    @file.setter
    def file(self, value):
        if isinstance(value, str):
            self._file = os.path.abspath(value)
            if os.path.exists(self._file):
                self.text = open(self.file, 'r').read()
        pass

    @property
    def query(self):
        """Get foramtted SQL text object that can be executed in database."""
        query = self.parse()
        return query

    @property
    def parallel(self):
        """Get parallel flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)):
            self._parallel = value
        pass

    def parse(self):
        """Parse into SQL text object."""
        text = self.text
        text = self._format(text)
        text = self._hintinize(text)
        query = sa.text(text)
        return query

    def execute(self, step):
        """Execute action."""
        query = self.query
        conn = self.db.connect()
        logger.info(f'Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        result = conn.execute(query)
        logger.info(f'SQL query completed')
        return result

    def _format(self, text):
        text = text.format(task=self.task)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spe.parse(text)
            tvalue = 'SELECT'
            ttype = spe.tokens.Keyword.DML
            parallel = self.parallel
            if parallel > 0:
                degree = '' if parallel is True else f'({parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spe.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            text = str(statements[0])
        return text

    pass


class Select(Extractable, DB, Item):
    """Represents SQL select as ETL Item."""

    def __init__(self, item_name=None, database=None, text=None, file=None,
                 columns=None, alias=None, parallel=False, fetch_size=1000):
        super().__init__(name=(item_name or __class__.__name__))
        self._database = None
        self._parallel = None
        self._text = None
        self._file = None
        self._columns = None
        self._alias = None
        self._fetch_size = None

        self.database = database
        self.parallel = parallel
        self.text = text
        self.file = file
        self.columns = columns
        self.alias = alias
        self.fetch_size = fetch_size
        pass

    @property
    def text(self):
        """Get raw SQL text."""
        return self._text

    @text.setter
    def text(self, value):
        if isinstance(value, str):
            self._text = to_sql(value)
        pass

    @property
    def file(self):
        """Get path to file containing select SQL text."""
        return self._file

    @file.setter
    def file(self, value):
        if isinstance(value, str):
            self._file = os.path.abspath(value)
            if os.path.exists(self._file):
                self.text = open(self.file, 'r').read()
        pass

    @property
    def columns(self):
        """Get list with configured column names."""
        return self._columns

    @columns.setter
    def columns(self, value):
        if isinstance(value, list):
            if all([el for el in value if isinstance(el, str)]):
                self._columns = value
        pass

    @property
    def parallel(self):
        """Get parallel flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)):
            self._parallel = value
        pass

    @property
    def query(self):
        """Get foramtted SQL text object that can be executed in database."""
        query = self.parse()
        return query

    @property
    def query_with_columns(self):
        """Get SQL text object with described columns."""
        columns = [sa.column(column) for column in self.describe()]
        query = self.query.columns(*columns)
        return query

    @property
    def query_with_alias(self):
        """Get SQL object with described columns and alias."""
        alias = self.alias or 's'
        query = self.query_with_columns.alias(name=alias)
        return query

    @property
    def fetch_size(self):
        """Get fetch size value."""
        return self._fetch_size

    @fetch_size.setter
    def fetch_size(self, value):
        if isinstance(value, int):
            self._fetch_size = value
        pass

    def parse(self):
        """Parse into SQL text object."""
        text = self.text
        text = self._format(text)
        text = self._hintinize(text)
        query = sa.text(text)
        return query

    def describe(self):
        """Get a real column list from the answerset."""
        conn = self.db.connect()
        query = self.query
        query = self._format(query)
        query = self._hintinize(query)
        query = to_sql(f'select * from ({query}) where 1 = 0')
        answerset = conn.execute(query)
        columns = answerset.keys()
        return columns

    def execute(self):
        """Execute SQL query in database."""
        conn = self.db.connect()
        query = self.parse()
        logger.info(f'Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        answerset = conn.execute(query)
        logger.info(f'SQL query completed')
        return answerset

    def fetch(self):
        """Fetch data from the query answerset."""
        answerset = self.execute()
        while True:
            dataset = answerset.fetchmany(self.fetch_size)
            if dataset:
                yield [dict(record) for record in dataset]
            else:
                break
        pass

    def extract(self, step):
        """Extract data."""
        return self.fetch()

    def _format(self, text):
        text = text.format(task=self.task)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spe.parse(text)
            tvalue = 'SELECT'
            ttype = spe.tokens.Keyword.DML
            parallel = self.parallel
            if parallel > 0:
                degree = '' if parallel is True else f'({parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spe.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            text = str(statements[0])
        return text

    pass


class Insert(Executable, DB, Item):
    """Represents SQL insert as ETL Item."""

    def __init__(self, item_name=None, database=None, schema=None, table=None,
                 select=None, purge=False, append=False, parallel=False):
        super().__init__(name=(item_name or __class__.__name__))
        self.database = database
        self.schema = schema
        self.table = table
        self.select = select
        self.purge = purge
        self.append = append
        self.parallel = parallel
        pass

    @property
    def schema(self):
        """Get inserting table schema name."""
        return self._schema

    @schema.setter
    def schema(self, value):
        if isinstance(value, str):
            self._schema = value.lower()
        elif value is None:
            self._schema = None
        pass

    @property
    def table(self):
        """Get inserting table name."""
        return self._table

    @table.setter
    def table(self, value):
        if isinstance(value, str):
            self._table = value.lower()
        elif value is None:
            self._table = None
        pass

    @property
    def address(self):
        """Get full inserting table address."""
        table = self.table
        table = table if self.schema is None else f'{self.schema}.{table}'
        table = table if self.db_link is None else f'{table}@{self.db_link}'
        return table

    @property
    def purge(self):
        """Get flag defining whether data purge is needed or not."""
        return self._purge

    @purge.setter
    def purge(self, value):
        if isinstance(value, bool):
            self._purge = value
        pass

    @property
    def append(self):
        """Get append flag."""
        return self._append

    @append.setter
    def append(self, value):
        if isinstance(value, (int, bool)) or value is None:
            self._append = value
        pass

    @property
    def parallel(self):
        """Get parallel flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)) or value is None:
            self._parallel = value
        pass

    @property
    def object(self):
        """Get object representing inserting table."""
        name = self.table
        meta = sa.MetaData()
        engine = self.db.engine
        table = sa.Table(name, meta, schema=self.schema,
                         autoload=True, autoload_with=engine)
        return table

    @property
    def text(self):
        """Get raw SQL select text."""
        return self._select

    @property
    def select(self):
        """Get formatted SQL select text."""
        return self._select

    @select.setter
    def select(self, value):
        if isinstance(value, str):
            if os.path.isfile(value):
                value = open(value, 'r').read()
            self._select = to_sql(value)
        elif value is None:
            self._select = None
        pass

    @property
    def query(self):
        """Get foramtted SQL text object that can be executed in database."""
        query = self.parse()
        return query

    def parse(self):
        """Parse into SQL text object."""
        table = self.object
        insert = table.insert()
        columns = [sa.column(column) for column in self.describe()]
        select = sa.text(f'\n{self.select}').columns(*columns)
        query = insert.from_select(columns, select)
        query = str(query)
        query = self._format(query)
        query = self._hintinize(query)
        return query

    def describe(self):
        """Get a real column list from the answerset."""
        conn = self.db.connect()
        query = self.select
        query = self._format(query)
        query = self._hintinize(query)
        query = to_sql(f'select * from ({query}) where 1 = 0')
        answerset = conn.execute(query)
        columns = answerset.keys()
        return columns

    def _format(self, text):
        text = text.format(task=self.task)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spe.parse(text)
            if self.parallel > 0:
                tvalue = 'SELECT'
                ttype = spe.tokens.Keyword.DML
                degree = '' if self.parallel is True else f'({self.parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spe.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            if self.append is True:
                tvalue = 'INSERT'
                ttype = spe.tokens.Keyword.DML
                hint = '/*+ append */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spe.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            text = str(statements[0])
        return text

    def prepare(self):
        """Prepare table."""
        self.logger = self._createlog()
        if self.purge is True:
            logger.debug(f'Table {self.table} will be purged')
            if self.db.vendor == 'oracle':
                self.truncate()
            else:
                self.delete()
        pass

    def delete(self):
        """Delete table data."""
        conn = self.db.connect()
        table = self.object
        query = table.delete()
        result = conn.execute(query)
        logger.info(f'{result.rowcount} {self.table} records deleted')
        pass

    def truncate(self):
        """Truncate table data."""
        conn = self.db.engine.connect()
        table = self.address
        query = sa.text(f'truncate table {table}')
        conn.execute(query)
        logger.info(f'Table {self.table} truncated')
        pass

    def execute(self, step):
        """Execute action."""
        conn = self.db.connect()
        query = self.parse()
        logger.info(f'Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        self._startlog(job_id=self.task.job_id,
                       run_id=self.task.run_id,
                       task_id=self.task.id,
                       step_id=step.id,
                       db_name=self.db.name,
                       table_name=self.table,
                       query_text=query)
        try:
            result = conn.execute(query)
        except sa.exc.SQLAlchemyError as error:
            self._endlog(error_text=error.orig.__str__())
            logger.info(f'SQL query failed')
            raise error
        else:
            self._endlog(output_rows=result.rowcount)
            logger.info(f'SQL query completed')
            return result.rowcount
        pass

    def _createlog(self):
        """Generate special file logger."""
        return self.task.logging.sql.setup()

    def _startlog(self, job_id=None, run_id=None, task_id=None, step_id=None,
                  db_name=None, table_name=None, query_text=None):
        """Start SQL logging."""
        self.logger.root.table.new()
        self.logger.table(job_id=job_id,
                          run_id=run_id,
                          task_id=task_id,
                          step_id=step_id,
                          db_name=db_name,
                          table_name=table_name,
                          query_type=self.__class__.__name__,
                          query_text=query_text,
                          start_date=dt.datetime.now())
        pass

    def _endlog(self, output_rows=None, output_text=None,
                error_code=None, error_text=None):
        """End SQL logging."""
        self.logger.table(end_date=dt.datetime.now(),
                          output_rows=output_rows,
                          output_text=output_text,
                          error_code=error_code,
                          error_text=error_text)
        pass

    pass


class File(Extractable, Loadable, OS, Item):
    """Represents file as ETL item."""

    def __init__(self, item_name=None, path=None, file_name=None,
                 encoding='utf-8', fetch_size=1000, purge=False):
        super().__init__(name=(item_name or __class__.__name__))
        self.path = path
        self.file_name = file_name
        self.encoding = encoding
        self.fetch_size = fetch_size
        self.purge = purge
        pass

    @property
    def path(self):
        """Get full file path."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str) or value is None:
            self._path = os.path.abspath(value) if value is not None else None
        pass

    @property
    def file_name(self):
        """Get formatted file name."""
        return self._file_name

    @file_name.setter
    def file_name(self, value):
        if isinstance(value, str) or value is None:
            self._file_name = tm.strftime(value) if value is not None else None
            if self.file_name is not None:
                self.path = os.path.join(self._path, self._file_name)
        pass

    @property
    def encoding(self):
        """Get file target encoding."""
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        if isinstance(value, str) or value is None:
            self._encoding = value
        pass

    @property
    def empty(self):
        """Check whether file is empty or not."""
        if os.path.getsize(self.path) == 0:
            return True
        else:
            return False
        pass

    @property
    def fetch_size(self):
        """Get fetch size value."""
        return self._fetch_size

    @fetch_size.setter
    def fetch_size(self, value):
        if isinstance(value, int) or value is None:
            self._fetch_size = value
        pass

    @property
    def purge(self):
        """Get purge flag."""
        return self._purge

    @purge.setter
    def purge(self, value):
        if isinstance(value, bool) or value is None:
            self._purge = value
        pass

    def delete(self):
        """Delete all file data."""
        open(self.path, 'w+').close()
        logger.info(f'File {self.path} was completely purged')
        pass

    def prepare(self):
        """Prepare file for ETL operation."""
        if self.purge is True:
            logger.debug(f'File {self.path} will be completely purged')
            self.delete()
        pass

    pass


class CSV(File):
    """Represents CSV file as ETL Item."""

    def __init__(self, item_name=None, path=None, file_name=None,
                 head=True, columns=None, delimiter=';', terminator='\r\n',
                 enclosure=None, trim=False, encoding='utf-8', fetch_size=1000,
                 purge=False):
        super().__init__(item_name=(item_name or __class__.__name__),
                         path=path, file_name=file_name, encoding=encoding,
                         fetch_size=fetch_size, purge=purge)
        self.head = head
        self.columns = columns
        self.delimiter = delimiter
        self.terminator = terminator
        self.enclosure = enclosure
        self.trim = trim
        pass

    @property
    def head(self):
        """Get header flag."""
        return self._head

    @head.setter
    def head(self, value):
        if isinstance(value, bool) or value is None:
            self._head = value
        pass

    @property
    def columns(self):
        """Get list with configured column names."""
        return self._columns

    @columns.setter
    def columns(self, value):
        if isinstance(value, list) or value is None:
            if value and all([el for el in value if isinstance(el, str)]):
                self._columns = value
            else:
                self._columns = None
        pass

    @property
    def delimiter(self):
        """Get column delimiter."""
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        if isinstance(value, str) or value is None:
            self._delimiter = value
        pass

    @property
    def terminator(self):
        """Get line terminator."""
        return self._terminator

    @terminator.setter
    def terminator(self, value):
        if isinstance(value, str) or value is None:
            self._terminator = value
        pass

    @property
    def enclosure(self):
        """Get column enclosure."""
        return self._enclosure

    @enclosure.setter
    def enclosure(self, value):
        if isinstance(value, str) or value is None:
            self._enclosure = value
        pass

    @property
    def trim(self):
        """Get trim flag."""
        return self._trim

    @trim.setter
    def trim(self, value):
        if isinstance(value, bool) or value is None:
            self._trim = value
        pass

    @property
    def dialect(self):
        """Generate CSV dialect based on configuration."""
        delimiter = self.delimiter
        quotechar = self.enclosure
        quoting = csv.QUOTE_NONE if quotechar is None else csv.QUOTE_ALL
        lineterminator = self.terminator
        skipinitialspace = self.trim
        dialect = {'delimiter': delimiter,
                   'quotechar': quotechar,
                   'quoting': quoting,
                   'lineterminator': lineterminator,
                   'skipinitialspace': skipinitialspace}
        return dialect

    def extract(self, step):
        """Extract data from CSV file."""
        with open(self.path, 'r', encoding=self.encoding) as fh:
            dialect = self.dialect
            fieldnames = self.columns
            reader = csv.DictReader(fh, fieldnames, **dialect)
            rows = [row for row in reader]
            length = len(rows)
            start = 0
            end = start+self.fetch_size
            while start < length:
                yield rows[start:end]
                start += self.fetch_size
                end = start+self.fetch_size
        pass

    def load(self, step, dataset):
        """Load data to CSV file."""
        with open(self.path, 'a', encoding=self.encoding, newline='') as fh:
            dialect = self.dialect
            fieldnames = [el for el in dataset[0]]
            writer = csv.DictWriter(fh, fieldnames, **dialect)
            if self.head is True and self.empty:
                writer.writeheader()
            writer.writerows(dataset)
        pass

    pass


class JSON(File):
    """Represents JSON file as ETL item."""

    def __init__(self, item_name=None, path=None, file_name=None,
                 encoding='utf-8', fetch_size=1000, purge=False):
        super().__init__(item_name=(item_name or __class__.__name__),
                         path=path, file_name=file_name, encoding=encoding,
                         fetch_size=fetch_size, purge=purge)
        pass

    def parse(self):
        """Parse JSON file and return its current content."""
        if not self.empty:
            with open(self.path, 'r', encoding=self.encoding) as fh:
                return json.load(fh)
        else:
            return []
        pass

    def extract(self, step):
        """Extract data from JSON file."""
        with open(self.path, 'r', encoding=self.encoding) as fh:
            rows = json.load(fh)
            length = len(rows)
            start = 0
            end = start+self.fetch_size
            while start < length:
                yield rows[start:end]
                start += self.fetch_size
                end = start+self.fetch_size
        pass

    def load(self, step, dataset):
        """Load data to JSON file."""
        content = self.parse()
        with open(self.path, 'w', encoding=self.encoding) as fh:
            json.dump(content+dataset, fh, indent=2)
        pass

    pass


class XML(File):
    """Represents XML file as ETL item."""

    def __init__(self, item_name=None, path=None, file_name=None,
                 encoding='utf-8', fetch_size=1000, purge=False):
        super().__init__(item_name=(item_name or __class__.__name__),
                         path=path, file_name=file_name, encoding=encoding,
                         fetch_size=fetch_size, purge=purge)

    def parse(self):
        """Parse XML file and/or get XML tree root."""
        if not self.empty:
            return xet.parse(self.path).getroot()
        else:
            return xet.Element('data')
        pass

    def extract(self, step):
        """Extract data from XML file."""
        root = self.parse()
        length = len(root)
        start = 0
        end = start+self.fetch_size
        while start < length:
            dataset = []
            for record in root[start:end]:
                dataset.append({field.tag: field.text for field in record})
            yield dataset
            start += self.fetch_size
            end = start+self.fetch_size
        pass

    def load(self, step, dataset):
        """Load data to XML file."""
        root = self.parse()
        for record in dataset:
            element = xet.SubElement(root, 'record')
            for key, value in record.items():
                field = xet.SubElement(element, key)
                field.text = str(value)
        with open(self.path, 'w', encoding=self.encoding) as fh:
            string = xet.tostring(root).decode(self.encoding)
            string = string.replace('  ', '')
            string = string.replace('\n', '')
            dom = xdom.parseString(string)
            dom.writexml(fh, addindent='  ', newl='\n')
        pass

    pass


class Files(Extractable, OS, Item):
    """Represents file sequence as ETL Item."""

    def __init__(self, item_name=None, server='localhost', path=None,
                 recursive=None, mask=None, created=None):
        super().__init__(name=(item_name or __class__.__name__))
        self.server = server
        self.path = path
        self.recursive = recursive
        self.mask = mask
        self.created = created
        pass

    @property
    def path(self):
        """Get files path."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str) or value is None:
            self._path = value
        pass

    @property
    def recursive(self):
        """Get recursive flag that allows to scan subdirectories."""
        return self._recursive

    @recursive.setter
    def recursive(self, value):
        if isinstance(value, bool) or value is None:
            self._recursive = value
        pass

    @property
    def mask(self):
        """Get file mask that helps to filter files by filenames."""
        return self._mask

    @mask.setter
    def mask(self, value):
        if isinstance(value, str) or value is None:
            self._mask = value
        pass

    @property
    def created(self):
        """Get date range that helps to filter files by created date."""
        return self._created

    @created.setter
    def created(self, value):
        if isinstance(value, (str, (dict, list, tuple))) or value is None:
            self._created = value
        pass

    def read(self):
        """Read full list of files."""
        for record in self.walk():
            path = record['path']
            logger.debug(f'{path} found')
            yield record
        pass

    def filter(self):
        """Generate filtered list of files."""
        date_from, date_to = self.between()
        logger.info(f'Using dates [{date_from}, {date_to}] to filter files')
        for record in self.read():
            path = record['path']
            mtime = record['mtime']
            # size = record['size']
            if self.mask is not None:
                if re.match(self.mask, os.path.basename(path)) is None:
                    logger.debug(f'{path} filtered by mask')
                    continue
            if mtime < date_from or mtime > date_to:
                logger.debug(f'{path} filtered by date')
                continue
            logger.debug(f'{path} matched')
            yield record
        pass

    def walk(self, dir=None):
        """Generate list of necessary files."""
        dir = dir or self.path
        if isinstance(self.server, Localhost) is True:
            dir = os.path.normpath(dir)
            if os.path.exists(dir) is True:
                for file in os.listdir(dir):
                    path = os.path.join(dir, file)
                    stats = os.stat(path)
                    isdir, isfile, mtime, size = self.explore(stats)
                    if isdir is True and self.recursive is True:
                        yield from self.walk(dir=path)
                    elif isfile is True or isdir is True:
                        root = os.path.normpath(self.path)
                        relpath = os.path.relpath(dir, self.path)
                        row = dict(server=self.server, path=path,
                                   root=root, dir=relpath, file=file,
                                   isdir=isdir, isfile=isfile,
                                   mtime=mtime, size=size)
                        yield row
        elif self.server.sftp is True:
            if self.server.exists(dir) is True:
                conn = self.server.connect()
                for stats in conn.sftp.listdir_attr(dir):
                    isdir, isfile, mtime, size = self.explore(stats)
                    if isdir is True and self.recursive is True:
                        yield from self.walk(dir=path)
                    elif isfile is True or isdir is True:
                        path = conn.sftp.normalize(f'{dir}/{stats.filename}')
                        relpath = os.path.relpath(dir, self.path)
                        row = dict(server=self.server, path=path,
                                   root=self.path, dir=relpath,
                                   file=stats.filename,
                                   isdir=isdir, isfile=isfile,
                                   mtime=mtime, size=size)
                        yield row
        # NOT FINISHED YET AND NOT RECOMMENDED TO USE WHEN ROOT HAVE FOLDERS
        # FTP support is pretty poor. So here not all FTP server will return
        # modification time and script will certainly fail in case of directory
        # found.
        elif self.server.ftp is True:
            if self.server.exists(dir) is True:
                conn = self.server.connect()
                for path in conn.ftp.nlst():
                    filename = os.path.basename(path)
                    relpath = '.'
                    isdir = False
                    isfile = True
                    mtime = conn.ftp.sendcmd(f'MDTM {path}')[4:]
                    mtime = dt.datetime.strptime(mtime, '%Y%m%d%H%M%S')
                    size = conn.ftp.size(path)
                    row = dict(host=self.server, path=path,
                               root=self.path, dir=relpath, filename=filename,
                               isdir=isdir, isfile=isfile,
                               mtime=mtime, size=size)
                    yield row
        pass

    def explore(self, stats):
        """Get main properties from file statistics."""
        isdir = stat.S_ISDIR(stats.st_mode)
        isfile = stat.S_ISREG(stats.st_mode)
        mtime = dt.datetime.fromtimestamp(stats.st_mtime)
        size = stats.st_size
        return (isdir, isfile, mtime, size)

    def between(self):
        """Parse date range used for file filtering."""
        today = getattr(self.task, 'calendar', None) or calendar.Today()
        if isinstance(self.created, (dict, list, tuple)):
            if isinstance(self.created, dict):
                date_from = self.created.get('date_from', 'None')
                date_to = self.created.get('date_to', 'None')
            elif isinstance(self.created, (list, tuple)):
                date_from = self.created[0]
                date_to = self.created[1]
            date_from = eval(date_from, {'calendar': today}) or dt.datetime.min
            date_to = eval(date_to, {'calendar': today}) or dt.datetime.max
        elif isinstance(self.created, str) is True:
            date_from = eval(self.created, {'calendar': today})
            date_to = dt.datetime.max
        else:
            date_from = dt.datetime.min
            date_to = dt.datetime.max
        return (date_from, date_to)

    def extract(self, step):
        """Extract data with file description."""
        yield list(self.filter())

    pass


class FileManager(Loadable, OS, Item):
    """Represents file manager as ETL Item."""

    def __init__(self, item_name=None, server='localhost', action='copy',
                 dest=None, nodir=False, zip=False, unzip=False,
                 tempname=False):
        super().__init__(name=(item_name or __class__.__name__))
        self.server = server
        self.action = action
        self.dest = dest
        self.nodir = nodir
        self.zip = zip
        self.unzip = unzip
        self.tempname = tempname
        pass

    @property
    def action(self):
        """Get action value that must be executed by file manager."""
        return self._action

    @action.setter
    def action(self, value):
        if isinstance(value, str) or value is None:
            self._action = value
        pass

    @property
    def dest(self):
        """Get destination file directories used for file processing."""
        return self._dest

    @dest.setter
    def dest(self, value):
        if isinstance(value, (str, list, tuple)) or value is None:
            if isinstance(value, str):
                self._dest = [value]
            elif isinstance(value, list):
                self._dest = value
            elif isinstance(value, tuple):
                self._dest = [item for item in value]
            elif value is None:
                self._dest = []
        pass

    @property
    def nodir(self):
        """Get flag defining whether directories must be processed or not."""
        return self._nodir

    @nodir.setter
    def nodir(self, value):
        if isinstance(value, bool) or value is None:
            self._nodir = value
        pass

    @property
    def zip(self):
        """Get flag defining whether files must be zipped or not."""
        return self._zip

    @zip.setter
    def zip(self, value):
        if isinstance(value, bool) or value is None:
            self._zip = value
        pass

    @property
    def unzip(self):
        """Get flag defining whether files must be unzipped or not."""
        return self._unzip

    @unzip.setter
    def unzip(self, value):
        if isinstance(value, bool) or value is None:
            self._unzip = value
        pass

    @property
    def tempname(self):
        """Get static name used for temporary files."""
        return self._tempname

    @tempname.setter
    def tempname(self, value):
        if isinstance(value, (str, bool)) or value is None:
            self._tempname = value
        pass

    def createlog(self):
        """Generate special file logger."""
        self.logger = self.task.logging.file.setup()
        pass

    def startlog(self, step, fileinfo):
        """Start particular file logging."""
        self.logger.root.table.new()
        self.logger.table(job_id=self.task.job_id,
                          run_id=self.task.run_id,
                          task_id=self.task.id,
                          step_id=step.id,
                          server=fileinfo['server'].host,
                          file_name=fileinfo['file'],
                          file_date=fileinfo['mtime'],
                          file_size=fileinfo['size'],
                          start_date=dt.datetime.now())
        pass

    def endlog(self):
        """End particular file logging."""
        self.logger.table(end_date=dt.datetime.now())
        pass

    def process(self, fileinfo):
        """Perform requested action for particular file."""
        server = fileinfo['server']
        path = fileinfo['path']
        logger.debug(f'Processing {path}')
        try:
            # Source and target are local.
            if (
                isinstance(self.server, Localhost)
                and isinstance(server, Localhost)
            ):
                logger.debug('On localhost by OS')
                if self.action in ('copy', 'move'):
                    self._copy_on_localhost(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source and target are the same remote, SSH enabled.
            elif (
                server == self.server
                and server.ssh is True
            ):
                logger.debug('On remote by SSH')
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ssh(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ssh(fileinfo)
            # Source and target are the same remote, SFTP enabled.
            elif (
                server == self.server
                and server.sftp is True
            ):
                logger.debug('On remote by SFTP and OS')
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are the same remote, FTP enabled.
            elif (
                server == self.server
                and server.ftp is True
            ):
                logger.debug('On remote by FTP and OS')
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source is local, target is remote, SFTP enabled.
            elif (
                server != self.server
                and isinstance(server, Localhost)
                and isinstance(self.server, Server)
                and self.server.sftp is True
            ):
                logger.debug('To remote by SFTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, SFTP enabled.
            elif (
                server != self.server
                and isinstance(server, Server)
                and isinstance(self.server, Localhost)
                and server.sftp is True
            ):
                logger.debug('To localhost by SFTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source is local, target is remote, FTP enabled.
            elif (
                server != self.server
                and isinstance(server, Localhost)
                and isinstance(self.server, Server)
                and self.server.ftp is True
            ):
                logger.debug('To remote by FTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, FTP enabled.
            elif (
                server != self.server
                and isinstance(server, Server) is True
                and isinstance(self.server, Localhost) is True
                and server.ftp is True
            ):
                logger.debug('To localhost by FTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP on both enabled.
            elif (
                server != self.server
                and isinstance(server, Server)
                and isinstance(self.server, Server)
                and server.sftp is True
                and self.server.sftp is True
            ):
                logger.debug('From remote by SFTP to remote by SFTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are different remotes, FTP on both enabled.
            elif (
                server != self.server
                and isinstance(server, Server)
                and isinstance(self.server, Server)
                and server.ftp is True
                and self.server.ftp is True
            ):
                logger.debug('From remote by FTP to remote by FTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, FTP enabled on source,
            # SFTP enabled on target.
            elif (
                server != self.server
                and isinstance(server, Server)
                and isinstance(self.server, Server)
                and server.ftp is True
                and self.server.sftp is True
            ):
                logger.debug('From remote by FTP to remote by SFTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_ftp_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP enabled on source,
            # FTP enabled on target.
            elif (
                server != self.server
                and isinstance(server, Server)
                and isinstance(self.server, Server)
                and server.sftp is True
                and self.server.ftp is True
            ):
                logger.debug('From remote by SFTP to remote by FTP')
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_sftp_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
        except Exception:
            logger.warning()
            pass
        pass

    def prepare(self):
        """Prepare file processing."""
        self.createlog()
        pass

    def load(self, step, dataset):
        """Load files into file manager and process them."""
        for fileinfo in dataset:
            self.startlog(step, fileinfo)
            self.process(fileinfo)
            self.endlog()
        pass

    def _copy_on_localhost(self, fileinfo):
        for path in self.dest:
            logger.debug(f'To {path}')
            if fileinfo['isfile'] is True:
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodir is False else '.'
                file = fileinfo['file']
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source, 'rb') as fhi:
                        with open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Unzipped {source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with gzip.open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Zipped {source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                shutil.copyfile(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'Copied {source} to {dest}')
        pass

    def _copy_on_remote_by_ssh(self, fileinfo):
        remote = fileinfo['server']
        conn = remote.connect()
        for path in self.dest:
            if fileinfo['isfile'] is True:
                logger.debug(f'To {path}')
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodir is False else '.'
                file = fileinfo['file']
                dest = f'{path}/{dir}'
                conn.execute(f'mkdir -p {dest}')
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    conn.execute(f'gunzip -c {source} > {temp or dest}')
                    if temp is not None:
                        conn.execute(f'mv {temp} {dest}')
                    logger.info(f'Unzipped {remote}:{source} '
                                f'to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    conn.execute(f'gzip -c {source} > {temp or dest}')
                    if temp is not None:
                        conn.execute(f'mv {temp} {dest}')
                    logger.info(f'Zipped {remote}:{source} to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.execute(f'cp {source} {temp or dest}')
                if temp is not None:
                    conn.execute(f'mv {temp} {dest}')
                logger.info(f'Copied {conn}:{source} to {conn}:{dest}')
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
            remote = fileinfo['server']
            conn = remote.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        conn.sftp.getfo(source, fhi)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            with open(temp or dest, 'wb') as fho:
                                shutil.copyfileobj(gz, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Unzipped {remote}:{source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(temp or dest, 'wb') as fho:
                        conn.sftp.getfo(source, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Zipped {remote}:{source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.sftp.get(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'Copied {remote}:{source} to {dest}')
        pass

    def _copy_from_localhost_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = self.server
            conn = remote.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = conn.sftp.normalize(f'{path}/{dir}')
                if remote.exists(dest) is False:
                    conn.sftp.mkdir(dest)
                dest = conn.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source, 'rb') as fhi:
                        conn.sftp.putfo(fhi, temp or dest)
                    if temp is not None:
                        conn.sftp.rename(temp, dest)
                    logger.info(f'Unzipped {source} to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with tempfile.TemporaryFile() as fho:
                            with gzip.GzipFile(filename=file, mode='wb',
                                               fileobj=fho) as gz:
                                shutil.copyfileobj(fhi, gz)
                            fho.seek(0)
                            conn.sftp.putfo(fho, temp or dest)
                    if temp is not None:
                        conn.sftp.rename(temp, dest)
                    logger.info(f'Zipped {source} to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.sftp.put(source, temp or dest)
                if temp is not None:
                    conn.sftp.rename(temp, dest)
                logger.info(f'Copied {source} to {remote}:{dest}')
        pass

    def _copy_from_remote_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = fileinfo['server']
            conn_in = remote_in.connect()
            remote_out = self.server
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = conn_out.sftp.normalize(f'{path}/{dir}')
                if remote_out.exists(dest) is False:
                    conn_out.sftp.mkdir(dest)
                dest = conn_out.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        conn_in.sftp.getfo(source, th)
                        th.seek(0)
                        with gzip.GzipFile(fileobj=th) as gz:
                            conn_out.sftp.putfo(gz, temp or dest)
                    if temp is not None:
                        conn_out.sftp.rename(temp, dest)
                    logger.info(f'Unzipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        with gzip.GzipFile(filename=file, mode='wb',
                                           fileobj=th) as gz:
                            conn_in.sftp.getfo(source, gz)
                        th.seek(0)
                        conn_out.sftp.putfo(th, temp or dest)
                    if temp is not None:
                        conn_out.sftp.rename(temp, dest)
                    logger.info(f'Zipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.sftp.getfo(source, th)
                    th.seek(0)
                    conn_out.sftp.putfo(th, temp or dest)
                if temp is not None:
                    conn_out.sftp.rename(temp, dest)
                logger.info(f'Copied {remote_in}:{source} to '
                            f'{remote_out}:{dest}')
        pass

    def _copy_from_localhost_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = self.server
            conn = remote.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if remote.exists(dest) is False:
                    conn.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source) as fhi:
                        with tempfile.TemporaryFile() as fho:
                            shutil.copyfileobj(fhi, fho)
                            fho.seek(0)
                            conn.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None:
                        conn.ftp.rename(temp, dest)
                    logger.info(f'Unzipped {source} to {remote}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with tempfile.TemporaryFile() as fho:
                            with gzip.GzipFile(filename=file, mode='wb',
                                               fileobj=fho) as gz:
                                shutil.copyfileobj(fhi, gz)
                            fho.seek(0)
                            conn.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None:
                        conn.ftp.rename(temp, dest)
                    logger.info(f'Zipped {source} to {remote}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(source, 'rb') as fhi:
                    conn.ftp.storbinary(f'STOR {temp or dest}', fhi)
                if temp is not None:
                    conn.ftp.rename(temp, dest)
                logger.info(f'Copied {source} to {remote}:{dest}')
        pass

    def _copy_from_remote_to_localhost_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote = fileinfo['server']
            conn = remote.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        conn.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            with open(temp or dest, 'wb') as fho:
                                shutil.copyfileobj(gz, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Unzipped {remote}:{source} to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        conn.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.open(temp or dest, 'wb') as fho:
                            shutil.copyfileobj(fhi, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'Zipped {remote}:{source} to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(temp or dest, 'wb') as fho:
                    conn.ftp.retrbinary(f'RETR {source}', fho.write)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'Copied {remote}:{source} to {dest}')
        pass

    def _copy_from_remote_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = fileinfo['server']
            conn_in = remote_in.connect()
            remote_out = self.server
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if remote_out.exists(dest) is False:
                    conn_out.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        conn_in.ftp.retrbinary(f'RETR {source}', th.write)
                        th.seek(0)
                        with gzip.GzipFile(fileobj=th) as gz:
                            conn_out.ftp.storbinary(f'STOR {temp or dest}', gz)
                    if temp is not None:
                        conn_out.ftp.rename(temp, dest)
                    logger.info(f'Unzipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as th:
                        with gzip.GzipFile(filename=file, mode='wb',
                                           fileobj=th) as gz:
                            conn_in.ftp.retrbinary(f'RETR {source}', gz.write)
                        th.seek(0)
                        conn_out.ftp.storbinary(f'STOR {temp or dest}', th)
                    if temp is not None:
                        conn_out.ftp.rename(temp, dest)
                    logger.info(f'Zipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    conn_out.ftp.storbinary(f'STOR {temp or dest}', th)
                if temp is not None:
                    conn_out.ftp.rename(temp, dest)
                logger.info(f'Copied {remote_in}:{source} '
                            f'to {remote_out}:{dest}')
        pass

    def _copy_from_remote_by_ftp_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = fileinfo['server']
            conn_in = remote_in.connect()
            remote_out = self.server
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = conn_out.sftp.normalize(f'{path}/{dir}')
                if remote_out.exists(dest) is False:
                    conn_out.sftp.mkdir(dest)
                dest = conn_out.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        conn_in.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            conn_out.sftp.putfo(gz, temp or dest)
                    if temp is not None:
                        conn_out.sftp.rename(temp, dest)
                    logger.info(f'Unzipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        with gzip.GzipFile(filename=file, mode='wb',
                                           fileobj=fhi) as gz:
                            conn_in.ftp.retrbinary(f'RETR {source}', gz.write)
                        fhi.seek(0)
                        conn_out.sftp.putfo(fhi, temp or dest)
                    if temp is not None:
                        conn_out.sftp.rename(temp, dest)
                    logger.info(f'Zipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    conn_out.sftp.putfo(th, temp or dest)
                if temp is not None:
                    conn_out.sftp.rename(temp, dest)
                logger.info(f'Copied {remote_in}:{source} '
                            f'to {remote_out}:{dest}')
        pass

    def _copy_from_remote_by_sftp_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = fileinfo['server']
            conn_in = remote_in.connect()
            remote_out = self.server
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                logger.debug(f'To {path}')
                dest = f'{path}/{dir}'
                if remote_out.exists(dest) is False:
                    conn_out.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fhi:
                        conn_in.sftp.getfo(source, fhi)
                        fhi.seek(0)
                        with gzip.GzipFile(fileobj=fhi) as gz:
                            conn_out.ftp.storbinary(f'STOR {temp or dest}', gz)
                    if temp is not None:
                        conn_out.ftp.rename(temp, dest)
                    logger.info(f'Unzipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with tempfile.TemporaryFile() as fho:
                        with gzip.GzipFile(filename=file, mode='wb',
                                           fileobj=fho) as gz:
                            conn_in.sftp.getfo(source, gz)
                        fho.seek(0)
                        conn_out.ftp.storbinary(f'STOR {temp or dest}', fho)
                    if temp is not None:
                        conn_out.ftp.rename(temp, dest)
                    logger.info(f'Zipped {remote_in}:{source} to '
                                f'{remote_out}:{dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.sftp.getfo(source, th)
                    th.seek(0)
                    conn_out.ftp.storbinary(f'STOR {dest}', th)
                if temp is not None:
                    conn_out.ftp.rename(temp, dest)
                logger.info(f'Copied {remote_in}:{source} '
                            f'to {remote_out}:{dest}')
        pass

    def _delete_on_localhost(self, fileinfo):
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            os.remove(path)
            logger.info(f'Deleted {path}')
        pass

    def _delete_on_remote_by_ssh(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.execute(f'rm -f {path}')
            logger.info(f'Deleted {remote}:{path}')
        pass

    def _delete_on_remote_by_sftp(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.sftp.remove(path)
            logger.info(f'Deleted {remote}:{path}')
        pass

    def _delete_on_remote_by_ftp(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.ftp.delete(path)
            logger.info(f'Deleted {remote}:{path}')
        pass

    pass
