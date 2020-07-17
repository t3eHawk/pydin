"""Contains Item prototypes and built-in Items."""

import csv
import os
import threading as th
import time as tm

import sqlalchemy as sa
import sqlparse as spe

from .core import Item
from .config import nodemaker
from .config import Database
from .logger import logger
from .utils import to_sql


class Base(Item):
    """Represents base class for all ETL Items."""

    extractable = False
    transformable = False
    loadable = False
    executable = False

    pass


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
            queue.put(dataset)
            step.records_read = len(dataset)
            logger.info(f'{step.records_read} records read')
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
                    self.load(step, dataset)
                except Exception:
                    logger.error()
                else:
                    step.records_written = len(dataset)
                    logger.info(f'{step.records_written} records written')
                    queue.task_done()
        pass

    pass


class Executable():
    """Represents executable Item."""

    executable = True

    def executor(self):
        """."""
        raise NotImplementedError
        pass

    def execute():
        """."""
        raise NotImplementedError
        pass

    pass


class Mapper(Transformable, Base):
    """Represents basic mapper used for data transformation."""

    def __init__(self, item_name=None):
        super().__init__(name=(item_name or __class__.__name__))
        pass

    def transform(self, input):
        """Transform data."""
        return input

    pass


class Table(Extractable, Loadable, Base):
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
    def db(self):
        """Describe database object (short)."""
        return self._database

    @property
    def database(self):
        """Describe database object."""
        return self._database

    @database.setter
    def database(self, value):
        if isinstance(value, Database):
            self._database = value
        elif isinstance(value, str):
            self._database = nodemaker.create(value)
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
        answerset = conn.execute(query)
        logger.info(f'{answerset.rowcount} {self.table_name} records deleted')
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
        return self.insert(dataset)

    pass

