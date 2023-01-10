"""Contains built-in models and prototypes."""

import csv
import datetime as dt
import gzip
import json
import os
import sys
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
from .utils import read_file_or_string
from .utils import sql_formatter, sql_compiler, sql_converter

from .core import Node


class Model(Node):
    """Represents single pipeline ETL model."""

    extractable = False
    transformable = False
    loadable = False
    executable = False

    def __init__(self, *args, model_name=None, source_name=None,
                 custom_query=None, date_field=None, days_back=None,
                 hours_back=None, months_back=None, timezone=None,
                 value_field=None, target_value=None,
                 min_value=None, max_value=None,
                 key_field=None, chunk_size=1000, cleanup=False, **kwargs):
        super().__init__(node_name=model_name, source_name=source_name)
        self.custom_query = custom_query
        if date_field:
            self.date_field = date_field
            self.days_back = days_back
            self.hours_back = hours_back
            self.months_back = months_back
            self.timezone = timezone
        if value_field:
            self.value_field = value_field
            self.target_value = target_value
            self.min_value, self.max_value = min_value, max_value
        self.key_field = key_field(self) if key_field else None
        self.chunk_size = chunk_size
        self.cleanup = cleanup
        self.configure(*args, **kwargs)

    @property
    def model_name(self):
        """Get model name."""
        return self.name

    @model_name.setter
    def model_name(self, value):
        self.name = value
        pass

    @property
    def key_field(self):
        """Key field of this model."""
        return self._key_field

    @key_field.setter
    def key_field(self, value):
        self._key_field = value

    @property
    def date_field(self):
        """Key field of this model."""
        if hasattr(self, '_date_field'):
            return self._date_field

    @date_field.setter
    def date_field(self, value):
        self._date_field = value

    @property
    def target_date(self):
        """Target date of this model."""
        if hasattr(self, 'date_field'):
            date = self.pipeline.calendar

            if isinstance(self.days_back, int):
                date = date.days_back(self.days_back)
            elif isinstance(self.hours_back, int):
                date = date.hours_back(self.hours_back)
            elif isinstance(self.months_back, int):
                date = date.months_back(self.months_back)

            if self.timezone:
                date.timezone = self.timezone

            return date

    @property
    def date_from(self):
        """The beggining of the target date of this model."""
        if hasattr(self, 'date_field'):
            return self.target_date.start

    @property
    def date_to(self):
        """The end of the target date of this model."""
        if hasattr(self, 'date_field'):
            return self.target_date.end

    @property
    def value_field(self):
        """Key field of this model."""
        if hasattr(self, '_value_field'):
            return self._value_field

    @value_field.setter
    def value_field(self, value):
        self._value_field = value

    @property
    def last_value(self):
        """."""
        if hasattr(self, 'value_field'):
            if self.target_value:
                return self.target_value
            elif self.min_value and self.max_value:
                return [self.min_value, self.max_value]
            else:
                return self.get_last_value()

    @property
    def custom_query(self):
        return self._custom_query

    @custom_query.setter
    def custom_query(self, value):
        if isinstance(value, str):
            value = self.read_custom_query(value)
            value = self.format_custom_query(value)
            self._custom_query = value
        else:
            self._custom_query = None

    @property
    def variables(self):
        """Get list of available variables as a dictionary."""
        data = {}
        if self.pipeline:
            data['pipeline'] = self.pipeline
            data['calendar'] = self.pipeline.calendar
            if self.job:
                data['job'] = self.job
                data['vars'] = self.job.data
        return data

    @property
    def recyclable(self):
        if hasattr(self, 'recycle'):
            return True
        else:
            return False

    def configure(self):
        """Configure this ETL model."""
        if self:
            raise NotImplementedError

    def prepare(self):
        """Do something with this ETL model before run."""
        pass

    def release(self):
        """Do something with this ETL model after run."""
        pass

    def create(self):
        """Create corresponding object in the data source."""
        if self:
            raise NotImplementedError

    def remove(self):
        """Remove corresponding object from the data source."""
        if self:
            raise NotImplementedError

    def clean(self):
        """Clean all data in the corresponding object."""
        if self:
            raise NotImplementedError

    def get_last_value(self):
        """Get last loaded value of this model."""
        if hasattr(self, 'value_field'):
            return self._get_last_value()

    def read_custom_query(self, value):
        """Read custom query."""
        if isinstance(value, str):
            if hasattr(self, '_read_custom_query'):
                return self._read_custom_query(value)
            else:
                return value

    def format_custom_query(self, value):
        """Format custom query."""
        if isinstance(value, str):
            if hasattr(self, '_format_custom_query'):
                return self._format_custom_query(value)
            else:
                return value

    def explain(self, parameter_name=None):
        """Get model or chosen parameter description."""
        if not parameter_name:
            return self.__doc__
        else:
            attribute = getattr(self, parameter_name)
            if attribute:
                return attribute.__doc__

    pass


class Extractable():
    """Represents extractable model."""

    extractable = True

    def to_extractor(self, step, queue):
        """Start model extractor."""
        name = f'{self}-Extractor'
        target = dict(target=self.extractor, args=(step, queue))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def extractor(self, step, queue):
        """Extract data."""
        logger.info(f'Reading records from {self}...')
        try:
            for dataset in self.extract(step):
                try:
                    queue.put(dataset)
                    step.records_read = len(dataset)
                    logger.info(f'{step.records_read} records read')
                except Exception:
                    step.records_error = len(dataset)
                    logger.info(f'{step.records_error} records with error')
                    logger.error()
                    step.error_handler()
                    if len(step.errors) >= step.pipeline.error_limit:
                        break
        except Exception:
            logger.error()
            step.error_handler()
        pass

    def extract(self):
        if self:
            raise NotImplementedError

    pass


class Transformable():
    """Represents transformable model."""

    transformable = True

    def to_transformer(self, step, input, output):
        """Start model transformer."""
        name = f'{self}-Transformer'
        target = dict(target=self.transformator, args=(step, input, output))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def transformator(self, step, input_queue, output_queue):
        """Transform data."""
        logger.info(f'Processing records of {self}...')
        while True:
            if input_queue.empty() is True:
                if step.extraction is True:
                    tm.sleep(0.001)
                    continue
                break
            else:
                input_records = input_queue.get()
                try:
                    output_records = list(map(self.transform, input_records))
                    output_queue.put(output_records)
                    step.records_processed = len(output_records)
                    logger.info(f'{step.records_processed} records processed')
                except Exception:
                    step.records_error = len(input_records)
                    logger.info(f'{step.records_error} records with error')
                    logger.error()
                    step.error_handler()
                    if len(step.errors) >= step.pipeline.error_limit:
                        break
                else:
                    input_queue.task_done()
        pass

    def transform(self):
        if self:
            raise NotImplementedError

    pass


class Loadable():
    """Represents loadable model."""

    loadable = True

    def to_loader(self, step, queue):
        """Start model loader."""
        name = f'{self}-Loader'
        target = dict(target=self.loader, args=(step, queue))
        self.thread = th.Thread(name=name, **target, daemon=True)
        step.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def loader(self, step, queue):
        """Load data."""
        logger.info(f'Writing records to {self}...')
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
                    step.records_written = len(coalesce(result, dataset))
                    logger.info(f'{step.records_written} records written')
                except Exception:
                    step.records_error = len(dataset)
                    logger.info(f'{step.records_error} records with error')
                    logger.error()
                    step.error_handler()
                    if len(step.errors) >= step.pipeline.error_limit:
                        break
                else:
                    queue.task_done()
        pass

    def load(self):
        if self:
            raise NotImplementedError

    pass


class Executable():
    """Represents executable model."""

    executable = True

    def to_executor(self, step):
        """Start model executor."""
        name = f'{self}-Executor'
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
            step.error_handler()
        pass

    def execute(self):
        if self:
            raise NotImplementedError

    pass


class Mapper(Transformable, Model):
    """Represents basic mapper used for data transformation."""

    def configure(self, func=None):
        """Configure transformation."""
        if func:
            self.transform = func
        pass

    def transform(self, input):
        """Transform data."""
        return input

    pass


class Table(Extractable, Loadable, Model):
    """Represents database table as ETL model."""

    def configure(self, schema_name=None, table_name=None, db_link=None,
                  append=False):
        self.schema_name = schema_name
        self.table_name = table_name
        self.db_link = db_link
        self.append = append
        pass

    @property
    def schema_name(self):
        """Schema owning the table."""
        return self._schema_name

    @schema_name.setter
    def schema_name(self, value):
        if isinstance(value, str):
            self._schema_name = value.lower()
        else:
            self._schema_name = None
        pass

    @property
    def table_name(self):
        """Name used to access the table in database."""
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        if isinstance(value, str):
            self._table_name = value.lower()
        else:
            self._table_name = None
        pass

    @property
    def db_link(self):
        """Database link used to access the table."""
        return self._db_link

    @db_link.setter
    def db_link(self, value):
        if isinstance(value, str):
            self._db_link = value.lower()
        else:
            self._db_link = None
        pass

    @property
    def reference(self):
        """Get table reference string: schema_name.table_name@db_link."""
        string = self.table_name
        if self.schema_name:
            string = f'{self.schema_name}.{string}'
        if self.db_link:
            string = f'{string}@{self.db_link}'
        return string

    @property
    def exists(self):
        """Check if table exists."""
        if self.db is not None:
            return self.db.engine.has_table(self.table_name)
        pass

    @property
    def append(self):
        """Flag defining whether append hint is needed or not."""
        return self._append

    @append.setter
    def append(self, value):
        if isinstance(value, bool):
            self._append = value
        else:
            self._append = None
        pass

    def get_table(self):
        """Get object representing table."""
        meta = sa.MetaData()
        engine = self.db.engine
        table = sa.Table(self.table_name, meta, schema=self.schema_name,
                         autoload=True, autoload_with=engine)
        return table

    def select(self):
        """Select table data."""
        conn = self.db.connect()
        query = sa.text(to_sql(f'select * from {self.reference}'))
        logger.info(f'Running SQL query <{self.table_name}>...')
        logger.line(f'-------------------\n{query}\n-------------------')
        answerset = conn.execute(query)
        logger.info(f'SQL query <{self.table_name}> completed')
        return answerset

    def fetch(self):
        """Fetch data from the table."""
        answerset = self.select()
        while True:
            dataset = answerset.fetchmany(self.chunk_size)
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
        query = sa.text(f'truncate table {self.reference}')
        conn.execute(query)
        logger.info(f'Table {self.table_name} truncated')
        pass

    def prepare(self):
        """Prepare model for ETL operation."""
        if self.cleanup is True:
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

    def recycle(self, key_value):
        """Perform the recycle of this node."""
        conn = self.db.connect()
        table = self.get_table()
        column = table.columns[self.key_field.label]
        delete = table.delete().where(column == key_value)
        conn.execute(delete)

    pass


class SQL(Executable, Model):
    """Represents SQL script as ETL model."""

    def configure(self, text=None, path=None, parallel=False):
        self.text = text
        self.path = path
        self.parallel = parallel

    @property
    def text(self):
        """Get raw SQL text."""
        return self.custom_query

    @text.setter
    def text(self, value):
        if isinstance(value, str):
            self.custom_query = value

    @property
    def path(self):
        """Get path to file containing select SQL text."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str):
            self._path = os.path.abspath(value)
            if os.path.exists(self._path):
                self.text = open(self.path, 'r').read()

    @property
    def query(self):
        """Get foramtted SQL text object that can be executed in database."""
        query = self.parse()
        return query

    @property
    def parallel(self):
        """Get parallel hint flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)):
            self._parallel = value

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
        text = text.format(**self.variables)
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


class Select(Extractable, Model):
    """Represents SQL select as ETL model."""

    def configure(self, text=None, path=None, columns=None, alias=None,
                  parallel=False):
        self.text = text
        self.path = path
        self.columns = columns
        self.alias = alias
        self.parallel = parallel

    @property
    def text(self):
        """Get raw SQL text."""
        return self.custom_query

    @text.setter
    def text(self, value):
        if isinstance(value, str):
            self.custom_query = value

    @property
    def path(self):
        """Get path to file containing select SQL text."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str):
            self._path = os.path.abspath(value)
            if os.path.exists(self._path):
                self.text = open(self.path, 'r').read()

    @property
    def columns(self):
        """Get list with configured column names."""
        return self._columns

    @columns.setter
    def columns(self, value):
        if isinstance(value, list):
            if all([el for el in value if isinstance(el, str)]):
                self._columns = value

    @property
    def parallel(self):
        """Get parallel flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)):
            self._parallel = value

    @property
    def query(self):
        """Get foramtted SQL text object that can be executed in database."""
        return self.parse()

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
        columns = [column for column in answerset.keys()]
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
            dataset = answerset.fetchmany(self.chunk_size)
            if dataset:
                yield [dict(record) for record in dataset]
            else:
                break

    def extract(self, step):
        """Extract data."""
        return self.fetch()

    def _format(self, text):
        text = text.format(**self.variables)
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


class Insert(Executable, Model):
    """Represents SQL insert as ETL model."""

    def configure(self, schema_name=None, table_name=None,
                  select=None, append=False, parallel=False):
        self.schema_name = schema_name
        self.table_name = table_name
        self.select = select
        self.append = append
        self.parallel = parallel

    def execute(self, step):
        """Perform the action of this node."""
        conn = self.db.connect()
        query = self.parse()
        logger.info(f'Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        self.startlog(job_id=self.job.id if self.task.job else None,
                      run_id=self.job.record_id if self.job else None,
                      task_id=self.task.id,
                      step_id=step.id,
                      db_name=self.db.name,
                      table_name=self.table_name,
                      query_text=query)
        try:
            result = conn.execute(query)
        except sa.exc.SQLAlchemyError as error:
            self.endlog(error_text=error.orig.__str__())
            logger.info(f'SQL query failed')
            raise error
        else:
            self.endlog(output_rows=result.rowcount)
            logger.info(f'SQL query completed')
            logger.info(f'{result.rowcount} records inserted')
            return result.rowcount

    def recycle(self, key_value):
        """Perform the recycle of this node."""
        conn = self.db.connect()
        table = self.proxy
        column = table.columns[self.key_field.label]
        delete = table.delete().where(column == key_value)
        conn.execute(delete)

    def prepare(self):
        """Prepare this node for the run."""
        self.logger = self.createlog()
        if self.cleanup is True:
            self.clean()

    def clean(self):
        """Remove all data from table."""
        logger.debug(f'Table {self.table_name} will be purged')
        if self.db.vendor == 'oracle':
            self.truncate()
        else:
            self.delete()

    @property
    def schema_name(self):
        """Get inserting table schema name."""
        return self._schema_name

    @schema_name.setter
    def schema_name(self, value):
        if isinstance(value, str):
            self._schema_name = value.lower()
        elif value is None:
            self._schema_name = None

    @property
    def table_name(self):
        """Get inserting table name."""
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        if isinstance(value, str):
            self._table_name = value.lower()
        elif value is None:
            self._ta_table_nameble = None

    @property
    def reference(self):
        """Get table reference string: schema_name.table_name@db_link."""
        string = self.table_name
        if self.schema_name:
            string = f'{self.schema_name}.{string}'
        if self.db_link:
            string = f'{string}@{self.db_link}'
        return string

    @property
    def proxy(self):
        """Get proxy object representing inserting table."""
        meta = sa.MetaData()
        engine = self.db.engine
        table = sa.Table(self.table_name, meta, schema=self.schema_name,
                         autoload=True, autoload_with=engine)
        return table

    @property
    def append(self):
        """Get append flag."""
        return self._append

    @append.setter
    def append(self, value):
        if isinstance(value, (int, bool)) or value is None:
            self._append = value

    @property
    def parallel(self):
        """Get parallel flag."""
        return self._parallel

    @parallel.setter
    def parallel(self, value):
        if isinstance(value, (int, bool)) or value is None:
            self._parallel = value

    @property
    def select(self):
        """Get formatted SQL select text."""
        return self.custom_query

    @select.setter
    def select(self, value):
        if isinstance(value, str):
            self.custom_query = value

    @property
    def text(self):
        """Get raw SQL text."""
        return self.assemble()

    @property
    def query(self):
        """Get formatted SQL text that can be executed in database."""
        return self.parse()

    def get_table(self):
        """Get object representing table."""
        return self.proxy

    def assemble(self):
        """Build raw SQL statement."""
        table = self.get_table()
        insert = table.insert()

        select = self.select
        columns = [sa.column(column) for column in self.describe()]

        if self.key_field:
            columns = [*columns, self.key_field.column]
            expression = f'{self.key_field.value} as {self.key_field.label}'
            select = sql_formatter(select, expand_select=expression)

        if self.date_field:
            date_from = sql_converter(self.date_from, self.db)
            date_to = sql_converter(self.date_to, self.db)
            between = f'{self.date_field} between {date_from} and {date_to}'
            select = sql_formatter(select, expand_where=between)

        if self.value_field:
            last_value = self.get_last_value()
            if last_value:
                comparison = f'{self.value_field} > {last_value}'
                select = sql_formatter(select, make_subquery=True)
                select = sql_formatter(select, expand_where=comparison)

        select = sa.text(f'\n{select}').columns(*columns)
        query = insert.from_select(columns, select)
        query = sql_compiler(query, self.db)
        return query

    def parse(self):
        """Parse final SQL statement."""
        query = self.assemble()
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
        columns = [column for column in answerset.keys()]
        return columns

    def delete(self):
        """Delete table data."""
        conn = self.db.connect()
        table = self.get_table()
        query = table.delete()
        result = conn.execute(query)
        logger.info(f'{result.rowcount} {self.table_name} records deleted')

    def truncate(self):
        """Truncate table data."""
        conn = self.db.engine.connect()
        query = sa.text(f'truncate table {self.reference}')
        conn.execute(query)
        logger.info(f'Table {self.table_name} truncated')

    def createlog(self):
        """Generate special logger."""
        return self.pipeline.logging.sql.setup()

    def startlog(self, job_id=None, run_id=None, task_id=None, step_id=None,
                 db_name=None, table_name=None, query_text=None):
        """Start special logging."""
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

    def endlog(self, output_rows=None, output_text=None,
                error_code=None, error_text=None):
        """End special logging."""
        self.logger.table(end_date=dt.datetime.now(),
                          output_rows=output_rows,
                          output_text=output_text,
                          error_code=error_code,
                          error_text=error_text)

    def _format(self, text):
        text = text.format(**self.variables)
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

    def _get_last_value(self):
        conn = self.db.connect()
        table = self.get_table()
        value_column = table.c[self.value_field]
        select = sa.select(sa.func.max(value_column))
        result = conn.execute(select).scalar()
        return result

    def _read_custom_query(self, value):
        if isinstance(value, str):
            return read_file_or_string(value)

    def _format_custom_query(self, value):
        if isinstance(value, str):
            return to_sql(value)

    pass


class File(Extractable, Loadable, Model):
    """Represents file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8'):
        self.file_name = file_name
        self.path = path
        self.encoding = encoding
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
    def path(self):
        """Get full file path."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str) or value is None:
            self._path = os.path.abspath(value) if value is not None else None
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

    def delete(self):
        """Delete all file data."""
        open(self.path, 'w+').close()
        logger.info(f'File {self.path} was completely purged')
        pass

    def prepare(self):
        """Prepare model ETL operation."""
        if self.cleanup is True:
            logger.debug(f'File {self.path} will be completely purged')
            self.delete()
        pass

    pass


class CSV(File):
    """Represents CSV file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8',
                  head=True, columns=None, delimiter=';', terminator='\r\n',
                  enclosure=None, trim=False):
        super().configure(file_name=file_name, path=path, encoding=encoding)
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

    def count(self):
        """Count the number of records in the source CSV file."""
        with open(self.path, 'r', encoding=self.encoding) as fh:
            reader = csv.DictReader(fh, self.columns, **self.dialect)
            total = sum(1 for i in reader)
            return total

    def extract(self, step):
        """Extract data from CSV file."""
        with open(self.path, 'r', encoding=self.encoding) as fh:
            reader = csv.DictReader(fh, self.columns, **self.dialect)
            total = self.count()
            chunk = []
            i = 1
            for record in reader:
                chunk.append(record)
                if i % self.chunk_size == 0 or i == total:
                    yield chunk
                    chunk = []
                i += 1
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
    """Represents JSON file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8'):
        super().configure(file_name=file_name, path=path, encoding=encoding)
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
            end = start+self.chunk_size
            while start < length:
                yield rows[start:end]
                start += self.chunk_size
                end = start+self.chunk_size
        pass

    def load(self, step, dataset):
        """Load data to JSON file."""
        content = self.parse()
        with open(self.path, 'w', encoding=self.encoding) as fh:
            json.dump(content+dataset, fh, indent=2)
        pass

    pass


class XML(File):
    """Represents XML file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8'):
        super().configure(file_name=file_name, path=path, encoding=encoding)
        pass

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
        end = start+self.chunk_size
        while start < length:
            dataset = []
            for record in root[start:end]:
                dataset.append({field.tag: field.text for field in record})
            yield dataset
            start += self.chunk_size
            end = start+self.chunk_size
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


class Files(Model):
    """Represents file sequence as ETL model."""

    def configure(self, path=None, mask=None, created=None, recursive=None,
                  server_name=None):
        self.path = path
        self.mask = mask
        self.created = created
        self.recursive = recursive
        self.server_name = server_name

    @property
    def path(self):
        """Get files path."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str) or value is None:
            self._path = value

    @property
    def recursive(self):
        """Get recursive flag that allows to scan subdirectories."""
        return self._recursive

    @recursive.setter
    def recursive(self, value):
        if isinstance(value, bool) or value is None:
            self._recursive = value

    @property
    def mask(self):
        """Get file mask that helps to filter files by filenames."""
        return self._mask

    @mask.setter
    def mask(self, value):
        if isinstance(value, str):
            self._mask = value
        elif value is None:
            self._mask = '*'

    @property
    def created(self):
        """Get date range that helps to filter files by created date."""
        return self._created

    @created.setter
    def created(self, value):
        if isinstance(value, (str, (dict, list, tuple))) or value is None:
            self._created = value

    @property
    def server_name(self):
        """Get file source server name."""
        return self._server_name

    @server_name.setter
    def server_name(self, value):
        if isinstance(value, str) or value is None:
            self._server_name = value

    @property
    def file_server(self):
        """Get file source connection proxy."""
        if not self.server_name:
            return self.server
        else:
            return connector.receive(self.server_name)

    def read(self):
        """Read full list of files."""
        for record in self.walk():
            path = record['path']
            logger.debug(f'File {path} found')
            yield record

    def filter(self):
        """Generate filtered list of files."""
        date_from, date_to = self.between()
        logger.info(f'Using mask {self.mask} to filter files')
        logger.info(f'Using dates {date_from} and {date_to} to filter files')
        for record in self.read():
            path = record['path']
            mtime = record['mtime']
            if self.mask and self.mask != '*':
                filename = os.path.basename(path)
                if not re.match(self.mask, filename):
                    logger.debug(f'File {path} filtered by mask')
                    continue
            if mtime < date_from or mtime > date_to:
                logger.debug(f'File {path} filtered by date')
                continue
            logger.debug(f'File {path} matched')
            yield record

    def walk(self, dir=None):
        """Generate list of necessary files."""
        dir = dir or self.path
        if isinstance(self.file_server, Localhost) is True:
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
                        row = dict(server=self.file_server, path=path,
                                   root=root, dir=relpath, file=file,
                                   isdir=isdir, isfile=isfile,
                                   mtime=mtime, size=size)
                        yield row
        elif self.file_server.sftp is True:
            if self.file_server.exists(dir) is True:
                conn = self.file_server.connect()
                for stats in conn.sftp.listdir_attr(dir):
                    isdir, isfile, mtime, size = self.explore(stats)
                    if isdir is True and self.recursive is True:
                        yield from self.walk(dir=path)
                    elif isfile is True or isdir is True:
                        path = conn.sftp.normalize(f'{dir}/{stats.filename}')
                        relpath = os.path.relpath(dir, self.path)
                        row = dict(server=self.file_server, path=path,
                                   root=self.path, dir=relpath,
                                   file=stats.filename,
                                   isdir=isdir, isfile=isfile,
                                   mtime=mtime, size=size)
                        yield row
        # NOT FINISHED YET AND NOT RECOMMENDED TO USE WHEN ROOT HAVE FOLDERS
        # FTP support is pretty poor. So here not all FTP server will return
        # modification time and script will certainly fail in case of directory
        # found.
        elif self.file_server.ftp is True:
            if self.file_server.exists(dir) is True:
                conn = self.file_server.connect()
                for path in conn.ftp.nlst():
                    filename = os.path.basename(path)
                    relpath = '.'
                    isdir = False
                    isfile = True
                    mtime = conn.ftp.sendcmd(f'MDTM {path}')[4:]
                    mtime = dt.datetime.strptime(mtime, '%Y%m%d%H%M%S')
                    size = conn.ftp.size(path)
                    row = dict(server=self.file_server, path=path,
                               root=self.path, dir=relpath, filename=filename,
                               isdir=isdir, isfile=isfile,
                               mtime=mtime, size=size)
                    yield row

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
        elif self.date_field:
            date_from = self.date_from
            date_to = self.date_to
        else:
            date_from = dt.datetime.min
            date_to = dt.datetime.max
        return (date_from, date_to)

    pass


class Filenames(Files, Extractable, Model):
    """Represents file names as ETL model."""

    def extract(self, step):
        """Extract data with file description."""
        yield from self.filter()

    pass


class FileManager(Files, Executable, Model):
    """Represents file manager as ETL model."""

    def configure(self, path=None, mask=None, created=None, recursive=None,
                  action='copy', dest=None, zip=False, unzip=False,
                  nodir=False, tempname=False, server_name=None):
        self.path = path
        self.mask = mask
        self.created = created
        self.recursive = recursive
        self.action = action
        self.dest = dest
        self.zip = zip
        self.unzip = unzip
        self.nodir = nodir
        self.tempname = tempname
        self.server_name = server_name

    @property
    def action(self):
        """Get action value that must be executed by file manager."""
        return self._action

    @action.setter
    def action(self, value):
        if isinstance(value, str) or value is None:
            if isinstance(value, str):
                requires = ['copy', 'move', 'delete']
                if value not in requires:
                    message = f'action must be {requires}, not {value}'
                    raise AttributeError(message)
            self._action = value

    @property
    def dest(self):
        """Get destination file directories used for file processing."""
        return self._dest

    @dest.setter
    def dest(self, values):
        if isinstance(values, (str, list, tuple)) or values is None:
            if isinstance(values, str):
                self._dest = [values]
            elif isinstance(values, list):
                self._dest = values
            elif isinstance(values, tuple):
                self._dest = [value for value in values]
            elif values is None:
                self._dest = []

    @property
    def nodir(self):
        """Get flag defining whether directories must be processed or not."""
        return self._nodir

    @nodir.setter
    def nodir(self, value):
        if isinstance(value, bool) or value is None:
            self._nodir = value

    @property
    def tempname(self):
        """Get static name used for temporary files."""
        return self._tempname

    @tempname.setter
    def tempname(self, value):
        if isinstance(value, (str, bool)) or value is None:
            self._tempname = value

    @property
    def zip(self):
        """Get flag defining whether files must be zipped or not."""
        return self._zip

    @zip.setter
    def zip(self, value):
        if isinstance(value, bool) or value is None:
            self._zip = value

    @property
    def unzip(self):
        """Get flag defining whether files must be unzipped or not."""
        return self._unzip

    @unzip.setter
    def unzip(self, value):
        if isinstance(value, bool) or value is None:
            self._unzip = value

    def createlog(self):
        """Generate special logger."""
        return self.pipeline.logging.file.setup()

    def startlog(self, step, fileinfo):
        """Start special logging."""
        self.logger.root.table.new()
        server_name = fileinfo['server'].host
        file_name = fileinfo['file']
        file_date = fileinfo['mtime']
        file_size = fileinfo['size']
        self.logger.table(job_id=self.job.id if self.job else None,
                          run_id=self.job.record_id if self.job else None,
                          task_id=self.task.id,
                          step_id=step.id,
                          server_name=server_name,
                          file_name=file_name,
                          file_date=file_date,
                          file_size=file_size,
                          start_date=dt.datetime.now())

    def endlog(self):
        """End special logging."""
        self.logger.table(end_date=dt.datetime.now())

    def process(self, fileinfo):
        """Perform requested action for particular file."""
        path = fileinfo['path']
        logger.debug(f'File {path} processing...')
        try:
            # Source and target are local.
            if (
                isinstance(self.server, Localhost)
                and isinstance(self.file_server, Localhost)
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_localhost(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source and target are the same remote, SSH enabled.
            elif (
                self.file_server == self.server
                and self.file_server.ssh is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ssh(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ssh(fileinfo)
            # Source and target are the same remote, SFTP enabled.
            elif (
                self.file_server == self.server
                and self.file_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are the same remote, FTP enabled.
            elif (
                self.file_server == self.server
                and self.file_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source is local, target is remote, SFTP enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Localhost)
                and isinstance(self.server, Server)
                and self.server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, SFTP enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server)
                and isinstance(self.server, Localhost)
                and self.file_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source is local, target is remote, FTP enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Localhost)
                and isinstance(self.server, Server)
                and self.server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, FTP enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server) is True
                and isinstance(self.server, Localhost) is True
                and self.file_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP on both enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server)
                and isinstance(self.server, Server)
                and self.file_server.sftp is True
                and self.server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are different remotes, FTP on both enabled.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server)
                and isinstance(self.server, Server)
                and self.file_server.ftp is True
                and self.server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, FTP enabled on source,
            # SFTP enabled on target.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server)
                and isinstance(self.server, Server)
                and self.file_server.ftp is True
                and self.server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_ftp_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP enabled on source,
            # FTP enabled on target.
            elif (
                self.file_server != self.server
                and isinstance(self.file_server, Server)
                and isinstance(self.server, Server)
                and self.file_server.sftp is True
                and self.server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_sftp_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
        except Exception:
            logger.warning()

    def prepare(self):
        """Prepare model for ETL operation."""
        self.logger = self.createlog()

    def execute(self, step):
        """Execute file manager action."""
        for fileinfo in self.filter():
            files_number = 1
            bytes_volume = fileinfo['size']
            step.files_read = files_number
            step.bytes_read = bytes_volume
            self.startlog(step, fileinfo)
            self.process(fileinfo)
            self.endlog()
            step.files_written = files_number
            step.bytes_written = bytes_volume

    def _copy_on_localhost(self, fileinfo):
        for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with open(source, 'rb') as fhi:
                        with gzip.open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                shutil.copyfile(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_on_remote_by_ssh(self, fileinfo):
        conn = self.file_server.connect()
        for path in self.dest:
            if fileinfo['isfile'] is True:
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
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    conn.execute(f'gzip -c {source} > {temp or dest}')
                    if temp is not None:
                        conn.execute(f'mv {temp} {dest}')
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.execute(f'cp {source} {temp or dest}')
                if temp is not None:
                    conn.execute(f'mv {temp} {dest}')
                logger.info(f'File {source} copied to {dest}')

    def _copy_on_remote_by_sftp(self, fileinfo):
        self._copy_from_remote_to_remote_by_sftp(fileinfo)

    def _copy_on_remote_by_ftp(self, fileinfo):
        self._copy_from_remote_to_remote_by_ftp(fileinfo)

    def _copy_from_remote_to_localhost_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.file_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(temp or dest, 'wb') as fho:
                        conn.sftp.getfo(source, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.sftp.get(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_localhost_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                dest = conn.sftp.normalize(f'{path}/{dir}')
                if self.server.exists(dest) is False:
                    conn.sftp.mkdir(dest)
                dest = conn.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname is True else None
                    with gzip.open(source, 'rb') as fhi:
                        conn.sftp.putfo(fhi, temp or dest)
                    if temp is not None:
                        conn.sftp.rename(temp, dest)
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                conn.sftp.put(source, temp or dest)
                if temp is not None:
                    conn.sftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = self.file_server
            remote_out = self.server
            conn_in = remote_in.connect()
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.sftp.getfo(source, th)
                    th.seek(0)
                    conn_out.sftp.putfo(th, temp or dest)
                if temp is not None:
                    conn_out.sftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_localhost_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
                dest = f'{path}/{dir}'
                if self.server.exists(dest) is False:
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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(source, 'rb') as fhi:
                    conn.ftp.storbinary(f'STOR {temp or dest}', fhi)
                if temp is not None:
                    conn.ftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_localhost_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.file_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with open(temp or dest, 'wb') as fho:
                    conn.ftp.retrbinary(f'RETR {source}', fho.write)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = self.file_server
            remote_out = self.server
            conn_in = remote_in.connect()
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    conn_out.ftp.storbinary(f'STOR {temp or dest}', th)
                if temp is not None:
                    conn_out.ftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_by_ftp_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            remote_in = self.file_server
            remote_out = self.server
            conn_in = remote_in.connect()
            conn_out = remote_out.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodir is False else '.'
            file = fileinfo['file']
            for path in self.dest:
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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.ftp.retrbinary(f'RETR {source}', th.write)
                    th.seek(0)
                    conn_out.sftp.putfo(th, temp or dest)
                if temp is not None:
                    conn_out.sftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

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
                    logger.info(f'File {source} unzipped to {dest}')
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
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname is True else None
                with tempfile.TemporaryFile() as th:
                    conn_in.sftp.getfo(source, th)
                    th.seek(0)
                    conn_out.ftp.storbinary(f'STOR {dest}', th)
                if temp is not None:
                    conn_out.ftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _delete_on_localhost(self, fileinfo):
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            os.remove(path)
            logger.info(f'File {path} deleted')

    def _delete_on_remote_by_ssh(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.execute(f'rm -f {path}')
            logger.info(f'File {path} deleted')

    def _delete_on_remote_by_sftp(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.sftp.remove(path)
            logger.info(f'File {path} deleted')

    def _delete_on_remote_by_ftp(self, fileinfo):
        remote = fileinfo['server']
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            remote.ftp.delete(path)
            logger.info(f'File {path} deleted')

    pass
