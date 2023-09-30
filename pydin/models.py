"""Contains built-in models and prototypes."""

import os
import stat
import shutil

import re
import time
import datetime as dt

import queue
import threading as th

import csv
import json
import gzip
import tempfile

import xml.etree.ElementTree as xet
import xml.dom.minidom as xdom

import sqlalchemy as sa
import sqlparse as spa

from .logger import logger
from .utils import calendar
from .utils import connector

from .utils import pause
from .utils import coalesce, timedelta
from .utils import read_file_or_string
from .utils import to_sql, sql_formatter, sql_compiler, sql_converter

from .core import Node
from .sources import Localhost, Server, Database


class Model(Node):
    """Represents a single node model."""

    extractable = False
    transformable = False
    loadable = False
    executable = False

    def __init__(self, *args, model_name=None, source_name=None,
                 custom_query=None, date_field=None, days_back=None,
                 hours_back=None, months_back=None, timezone=None,
                 value_field=None, target_value=None,
                 min_value=None, max_value=None,
                 key_field=None, insert_key_field=True,
                 chunk_size=1000, parallelism=0, cleanup=False, **kwargs):
        super().__init__(node_name=model_name, source_name=source_name)
        self.workers = []
        self.lock = th.Lock()
        self.object_queue = queue.Queue()
        self.data_queue = queue.Queue()
        self.extractions_done = 0
        self.transformations_done = 0
        self.loadings_done = 0
        self.datastream_number = 0
        self.total_records = 0
        self.total_duration = 0
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
        self.insert_key_field = insert_key_field
        self.chunk_size = chunk_size
        self.parallelism = parallelism
        self.cleanup = cleanup
        self.configure(*args, **kwargs)

    @property
    def model_name(self):
        """Get model name."""
        return self.name

    @model_name.setter
    def model_name(self, value):
        self.name = value

    @property
    def model_type(self):
        return self.__class__.__name__

    @property
    def returns(self):
        """Check if the data source is still has something to return."""
        if self.parallelism:
            return self.queuing
        elif not self.parallelism:
            return self.passed
        return False

    @property
    def queuing(self):
        """Check if data objects are being queued at the moment."""
        if not self.object_queue.empty() or not self.data_queue.empty():
            return True
        return False

    @property
    def passed(self):
        """Check if data object is passed and has nothing to return."""
        if not self.parallelism and not self.extractions_done:
            return True
        return False

    @property
    def extracting(self):
        """Get current extraction state."""
        raise NotImplementedError

    @property
    def transforming(self):
        """Get current transformation state."""
        raise NotImplementedError

    @property
    def loading(self):
        """Get current loading state."""
        raise NotImplementedError

    @property
    def working(self):
        """Check if data workers are doing something at the moment."""
        for worker in self.workers:
            if worker.is_alive():
                return True
        return False

    @property
    def recyclable(self):
        """Identifies if the model is recyclable type or not."""
        if hasattr(self, 'recycle'):
            return True
        return False

    @property
    def speed(self):
        """Calculate total operations speed of this model."""
        if self.total_records and self.total_duration:
            return round(self.total_records/self.total_duration)
        return 0

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
        """."""
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
    def logging(self):
        """Get the available logging interface for this model."""
        if self.pipeline:
            if isinstance(self, Query):
                return self.pipeline.logging.query
            elif isinstance(self, (File, Files)):
                return self.pipeline.logging.file

    def configure(self, *args, **kwargs):
        """Configure this ETL model."""
        raise NotImplementedError

    def begin(self, *args, **kwargs):
        """Open as a new object within the given structure."""
        if self.logging:
            self.logging.setup()
            self.logging.start(self, *args, **kwargs)

    def prepare(self):
        """Do something with this ETL model before run."""

    def release(self):
        """Do something with this ETL model after run."""

    def create(self):
        """Create corresponding object in the data source."""

    def remove(self):
        """Remove corresponding object from the data source."""

    def clean(self):
        """Clean all data in the corresponding object."""

    def finish(self, *args, **kwargs):
        """Save as a closed object in the end of work."""
        if self.logging:
            self.logging.end(self, *args, **kwargs)

    def parallelize(self, target_operation, *args, **kwargs):
        """Prepare the data workers used for parallel operations."""
        parent = th.current_thread()
        params = {'args': args, 'kwargs': kwargs}
        for i in range(self.parallelism):
            seqno = i+1
            name = f'{parent.name}-{seqno}'
            worker = th.Thread(name=name, target=target_operation, **params)
            worker.start()
            self.workers.append(worker)
        return True

    def wait(self):
        """Wait until all data workers are done."""
        for worker in self.workers:
            worker.join()

    def itemize(self):
        """Generate an item table with the data stream parameters."""

    def parametrize(self):
        """Generate parameters for the data stream."""

    def charge(self):
        """Queue the data objects returned by the data listing function."""
        for data_object in self.itemize():
            self.object_queue.put(data_object)
        return True

    def get_last_value(self):
        """Get last loaded value of this model."""
        if hasattr(self, 'value_field'):
            if hasattr(self, '_get_last_value'):
                return self._get_last_value()

    def get_custom_query(self):
        """Get custom query."""
        return self.custom_query

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

    def enrich(self, dataset):
        """Enrich dataset with some configurable modifications."""
        if self.key_field and self.insert_key_field:
            for record in dataset:
                record[self.key_field.label] = self.key_field.value
        return dataset

    def first(self):
        """Get the very first record from the data object."""
        raise NotImplementedError

    def count(self):
        """Get the total number of records in the data object."""
        raise NotImplementedError

    def volume(self):
        """Get the volume of the data object."""
        raise NotImplementedError

    def pack(self, something, tag=None, payload=None):
        """Pack the given data into the special data package."""
        if isinstance(something, DataPackage):
            return something
        datapack = DataPackage(tag=tag, data=something, payload=payload)
        return datapack

    def explain(self, parameter_name=None):
        """Get model or chosen parameter description."""
        if not parameter_name:
            return self.__doc__
        else:
            parameter = getattr(self, parameter_name)
            if parameter:
                return parameter.__doc__


class Extractable():
    """Represents extractable model."""

    extractable = True

    def to_extractor(self, step, queue):
        """Start base extractor."""
        name = f'{self}-Extractor'
        target, args = self.target_extractor(), (step, queue)
        thread = th.Thread(name=name, target=target, args=args, daemon=True)
        logger.debug(f'Starting {thread.name}...')
        self.step, self.thread = step, thread
        self.step.threads.append(thread)
        return self.thread.start()

    def extractor(self, step, queue):
        """Extract data into the queue."""
        logger.info(f'Reading records from {self}...')
        while step.alive:
            try:
                for dataset in self.target_extract():
                    start_date = dt.datetime.now()
                    datapack = self.pack(dataset)
                    size = datapack.calculate()
                    try:
                        queue.put(datapack)
                        with self.lock:
                            step.records_read = size
                            self.total_records += size
                            self.total_duration += timedelta(start_date)
                        logger.info(f'{step.records_read} records read, '
                                    f'{self.total_duration:.4f} sec, '
                                    f'{self.speed} rec/sec')
                    except Exception:
                        with self.lock:
                            step.records_error = size
                        logger.info(f'{step.records_error} records with error')
                        logger.error()
                        step.error_handler()
                        if len(step.errors) >= step.pipeline.error_limit:
                            break
            except Exception:
                logger.error()
                step.error_handler()
            with self.lock:
                self.extractions_done += 1
            pause()

    def extract(self):
        """Extract data from the data source."""
        raise NotImplementedError

    def extracts(self, step):
        """Get an extractor for parallel operations."""
        if self.parallelism:
            return self.extractor(step, self.data_queue)

    def extractors(self, step, queue):
        """Extract data from the data source in a parallel mode."""
        if self.parallelism:
            if self.charge():
                if self.parallelize(self.extracts, step):
                    while self.working:
                        try:
                            if not self.data_queue.empty():
                                datapack = self.data_queue.get()
                                queue.put(datapack)
                                self.data_queue.task_done()
                        except Exception:
                            logger.error()
                        pause()
                    self.wait()

    def extract_many(self):
        """Extract data from the data stream."""
        while not self.object_queue.empty():
            parameters = self.object_queue.get()
            data_stream = self.DataStream(self, **parameters)
            tag, payload = data_stream.identify(), data_stream.detail()
            for dataset in data_stream.extract():
                datapack = self.pack(dataset, tag=tag, payload=payload)
                yield datapack
            pause()

    def target_extractor(self):
        """Get target extraction handler."""
        if self.parallelism:
            return self.extractors
        return self.extractor

    def target_extract(self):
        """Get target extraction method."""
        if self.parallelism:
            return self.extract_many()
        return self.extract()


class Transformable():
    """Represents transformable model."""

    transformable = True

    def to_transformer(self, step, input_queue, output_queue):
        """Start base transformer."""
        name = f'{self}-Transformer'
        target, args = self.transformator, (step, input_queue, output_queue)
        thread = th.Thread(name=name, target=target, args=args, daemon=True)
        logger.debug(f'Starting {thread.name}...')
        self.step, self.thread = step, thread
        self.step.threads.append(thread)
        return self.thread.start()

    def transformator(self, step, input_queue, output_queue):
        """Transform data from the input queue to the output queue."""
        logger.info(f'Processing records of {self}...')
        while step.extracting or not input_queue.empty():
            if not input_queue.empty():
                start_date = dt.datetime.now()
                datapack = input_queue.get()
                input_dataset = datapack.get()
                size = datapack.calculate()
                try:
                    output_dataset = list(map(self.transform, input_dataset))
                    datapack.put(output_dataset)
                    output_queue.put(datapack)
                    with self.lock:
                        step.records_processed = size
                        self.total_records += size
                        self.total_duration += timedelta(start_date)
                    logger.info(f'{step.records_processed} records processed, '
                                f'{self.total_duration:.4f} sec, '
                                f'{self.speed} rec/sec')
                except Exception:
                    with self.lock:
                        step.records_error = datapack.size
                    logger.info(f'{step.records_error} records with error')
                    logger.error()
                    step.error_handler()
                    if len(step.errors) >= step.pipeline.error_limit:
                        break
                else:
                    input_queue.task_done()
            with self.lock:
                self.transformations_done += 1
            pause()

    def transform(self):
        """Transform data from one data source for another."""
        raise NotImplementedError


class Loadable():
    """Represents loadable model."""

    loadable = True

    def to_loader(self, step, queue):
        """Start base loader."""
        name = f'{self}-Loader'
        target, args = self.target_loader(), (step, queue)
        thread = th.Thread(name=name, target=target, args=args, daemon=True)
        logger.debug(f'Starting {thread.name}...')
        self.step, self.thread = step, thread
        self.step.threads.append(thread)
        return self.thread.start()

    def loader(self, step, queue):
        """Load data from the queue."""
        logger.info(f'Writing records to {self}...')
        while step.processing or not queue.empty():
            if not queue.empty():
                start_date = dt.datetime.now()
                datapack = queue.get()
                size = datapack.calculate()
                try:
                    initial_data = datapack.get()
                    final_data = self.enrich(initial_data)
                    datapack.put(final_data)
                    result = self.target_load(datapack)
                    with self.lock:
                        step.records_written = coalesce(result, size)
                        self.total_records += size
                        self.total_duration += timedelta(start_date)
                    logger.info(f'{step.records_written} records written, '
                                f'{self.total_duration:.4f} sec, '
                                f'{self.speed} rec/sec')
                except Exception:
                    with self.lock:
                        step.records_error = size
                    logger.info(f'{step.records_error} records with error')
                    logger.error()
                    step.error_handler()
                    if len(step.errors) >= step.pipeline.error_limit:
                        break
                else:
                    queue.task_done()
            with self.lock:
                self.loadings_done += 1
            pause()

    def load(self, dataset):
        """Load data into the data source."""
        raise NotImplementedError

    def loads(self, step):
        """Get a loader for parallel operations."""
        if self.parallelism:
            return self.loader(step, self.data_queue)

    def loaders(self, step, queue):
        """Load data into the data source in a parallel mode."""
        if self.parallelism:
            if self.parallelize(self.loads, step):
                while step.processing or not queue.empty():
                    try:
                        if not queue.empty():
                            datapack = queue.get()
                            self.data_queue.put(datapack)
                            queue.task_done()
                    except Exception:
                        logger.error()
                    pause()
                self.wait()

    def load_many(self, datapack):
        """Load data into the data stream."""
        if self.parallelism:
            tag, dataset, payload = datapack.expand()
            parameters = self.parametrize(tag, payload)
            data_stream = self.DataStream(self, **parameters)
            result = data_stream.load(dataset)
            return result

    def target_loader(self):
        """Get target loading handler."""
        if self.parallelism:
            return self.loaders
        return self.loader

    def target_load(self, datapack):
        """Get target loading method."""
        if self.parallelism:
            return self.load_many(datapack)
        return self.load(datapack.data)


class Executable():
    """Represents executable model."""

    executable = True

    def to_executor(self, step):
        """Start model executor."""
        name = f'{self}-Executor'
        target, args = self.executor, (step,)
        thread = th.Thread(name=name, target=target, args=args, daemon=True)
        logger.debug(f'Starting {thread.name}...')
        step.threads.append(thread)
        self.step = step
        self.thread = thread
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

    def execute(self, step):
        """Base execute method."""
        raise NotImplementedError


class DataPackage:
    """Represents a single data package transferred over a pipeline."""

    def __init__(self, tag=None, data=None, payload=None):
        self.tag = tag
        self.data = data
        self.payload = payload

    def __len__(self):
        if self.data and isinstance(self.data, list):
            return len(self.data)
        return 0

    @property
    def size(self):
        """Get the size of the data in this package."""
        return self.calculate()

    def identify(self, tag):
        """Give the tag to this data package."""
        self.tag = tag

    def parametrize(self, payload):
        """Add some payload to this data package."""
        self.payload = payload

    def put(self, data):
        """Put data inside this package."""
        self.data = data

    def get(self):
        """Get data from this package."""
        return self.data

    def calculate(self):
        """Calclulate the size of the data in this package."""
        return len(self)

    def expand(self):
        """Expand this data package into the list."""
        return [self.tag, self.data, self.payload]


class DataStream:
    """Represents a separate data stream within a data object."""

    def __init__(self, model, *args, **kwargs):
        self.model = model
        with self.model.lock:
            self.model.datastream_number += 1
            self.seqno = self.model.datastream_number
        self.configure(*args, **kwargs)

    @property
    def tag(self):
        """Get data stream tag."""
        return self.identify()

    def configure(self, *args, **kwargs):
        """Configure this data stream."""
        raise NotImplementedError

    def identify(self):
        """Identify this data stream with a tag."""
        raise NotImplementedError

    def detail(self):
        """Detail this data stream with some properties."""
        raise NotImplementedError

    def extract(self):
        """Extract data from this data stream."""
        raise NotImplementedError

    def load(self):
        """Load data into this data stream."""
        raise NotImplementedError


class Mapper(Transformable, Model):
    """Represents basic mapper used for data transformation."""

    def configure(self, func=None):
        """Configure transformation."""
        if func:
            self.transform = func

    def transform(self, record):
        """Transform data."""
        return record

    pass


class Table(Extractable, Loadable, Model):
    """Represents database table as ETL model."""

    def configure(self, schema_name=None, table_name=None, db_link=None,
                  fetch_size=None, commit_size=None, append=False):
        self.schema_name = schema_name
        self.table_name = table_name
        self.db_link = db_link
        self.fetch_size = fetch_size
        self.commit_size = commit_size
        self.append = append

    def prepare(self):
        """Prepare model for ETL operation."""
        if self.cleanup is True:
            logger.debug(f'Table {self.table_name} will be purged')
            if self.db.vendor == 'oracle':
                self.truncate()
            else:
                self.delete()

    def extract(self):
        """Extract data."""
        return self.fetch()

    def load(self, dataset):
        """Load data."""
        self.insert(dataset)

    def recycle(self, key_value):
        """Perform the recycle of this node."""
        conn = self.db.connect()
        table = self.get_table()
        column = table.columns[self.key_field.label]
        delete = table.delete().where(column == key_value)
        conn.execute(delete)

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

    @property
    def fetch_size(self):
        """Get fetch size value."""
        return self.chunk_size

    @fetch_size.setter
    def fetch_size(self, value):
        if isinstance(value, int):
            self.chunk_size = value

    @property
    def commit_size(self):
        """Get commit size value."""
        return self.chunk_size

    @commit_size.setter
    def commit_size(self, value):
        if isinstance(value, int):
            self.chunk_size = value

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
            dataset = answerset.fetchmany(self.fetch_size)
            if dataset:
                yield [dict(record) for record in dataset]
            else:
                break

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

    def truncate(self):
        """Truncate table data."""
        conn = self.db.engine.connect()
        query = sa.text(f'truncate table {self.reference}')
        conn.execute(query)
        logger.info(f'Table {self.table_name} truncated')

    pass


class Query(Model):
    """Represents base query as ETL model."""

    def __init__(self, *args, **kwargs):
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.query_type = None
        self.query_text = None
        self.output_rows = None
        self.output_text = None
        self.error_code = None
        self.error_text = None
        super().__init__(*args, **kwargs)

    def rewrite(self, text):
        """Rewrite the query text with the given value."""
        self.query_text = text
        return self.query_text

    def save_as_completed(self, result_value=None):
        """Save as a successfully completed object."""
        self.output_rows = result_value
        self.error_code = None
        self.error_text = None
        return self.finish()

    def save_as_failed(self, error_description=None):
        """Save as an error object."""
        self.output_rows = None
        self.error_code = None
        self.error_text = error_description
        return self.finish()


class SQL(Executable, Query):
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
        query = self.assemble()
        query = self._format(query)
        query = self._hintinize(query)
        query = self.rewrite(query)
        return query

    def assemble(self):
        """Build raw SQL statement."""
        query = sa.text(self.text)
        query = sql_compiler(query, self.db)
        query = sql_formatter(query, fix_new_lines=True)
        return query

    def execute(self, step):
        """Execute action."""
        conn = self.db.connect().execution_options(autocommit=True)
        query = self.query
        logger.info('Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        self.begin()
        try:
            result = conn.execute(query)
        except sa.exc.SQLAlchemyError as error:
            error_description = str(error.__dict__['orig'])
            self.save_as_failed(error_description=error_description)
            logger.info('SQL query failed')
            raise error
        else:
            result_value = result.rowcount
            self.save_as_completed(result_value=result_value)
            logger.info(f'{result_value} records processed')
            logger.info('SQL query completed')
            return result

    def _format(self, text):
        text = text.format(**self.variables)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spa.parse(text)
            tvalue = 'SELECT'
            ttype = spa.tokens.Keyword.DML
            parallel = self.parallel
            if parallel > 0:
                degree = '' if parallel is True else f'({parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spa.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            text = str(statements[0])
        return text

    pass


class Select(Extractable, Query):
    """Represents SQL select as ETL model."""

    def configure(self, text=None, path=None, columns=None, alias=None,
                  fetch_size=None, parallel=False):
        self.text = text
        self.path = path
        self.columns = columns
        self.fetch_size = fetch_size
        self.alias = alias
        self.parallel = parallel

    def extract(self):
        """Extract data."""
        return self.fetch()

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
    def fetch_size(self):
        """Get fetch size value."""
        return self.chunk_size

    @fetch_size.setter
    def fetch_size(self, value):
        if isinstance(value, int):
            self.chunk_size = value

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
        query = sa.text(self.query).columns(*columns)
        return query

    @property
    def query_with_alias(self):
        """Get SQL object with described columns and alias."""
        alias = self.alias or 's'
        query = self.query_with_columns.alias(name=alias)
        return query

    def parse(self):
        """Parse into SQL text object."""
        query = self.assemble()
        query = self._format(query)
        query = self._hintinize(query)
        query = self.rewrite(query)
        return query

    def assemble(self):
        """Build raw SQL statement."""
        query = self.text
        columns = [sa.column(column) for column in self.describe()]

        if self.date_field:
            date_from = sql_converter(self.date_from, self.db)
            date_to = sql_converter(self.date_to, self.db)
            between = f'{self.date_field} between {date_from} and {date_to}'
            query = sql_formatter(query, expand_where=between)

        if self.value_field:
            last_value = self.get_last_value()
            if last_value:
                comparison = f'{self.value_field} > {last_value}'
                query = sql_formatter(query, make_subquery=True)
                query = sql_formatter(query, expand_where=comparison)

        query = sa.text(query).columns(*columns)
        query = sql_compiler(query, self.db)
        query = sql_formatter(query)
        return query

    def describe(self):
        """Get a real column list from the answerset."""
        conn = self.db.connect()
        query = self.text
        query = self._format(query)
        query = self._hintinize(query)
        query = sql_formatter(query, make_empty=True, db=self.db)
        result = conn.execute(query)
        columns = [column for column in result.keys()]
        return columns

    def execute(self):
        """Execute SQL query in database."""
        conn = self.db.connect()
        query = self.parse()
        logger.info('Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        self.begin()
        result = conn.execute(query)
        logger.info('SQL query completed')
        return result

    def fetch(self):
        """Fetch data from the query answerset."""
        try:
            result = self.execute()
            result_value = 0
            while True:
                dataset = result.fetchmany(self.fetch_size)
                if dataset:
                    result_value += len(dataset)
                    yield [dict(record) for record in dataset]
                else:
                    break
        except sa.exc.SQLAlchemyError as error:
            error_description = str(error.__dict__['orig'])
            self.save_as_failed(error_description=error_description)
            logger.info('SQL query failed')
            raise error
        else:
            self.save_as_completed(result_value=result_value)
            logger.info(f'{result_value} records fetched')

    def _format(self, text):
        text = text.format(**self.variables)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spa.parse(text)
            tvalue = 'SELECT'
            ttype = spa.tokens.Keyword.DML
            parallel = self.parallel
            if parallel > 0:
                degree = '' if parallel is True else f'({parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spa.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            text = str(statements[0])
        return text


class Insert(Executable, Query):
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
        logger.info('Running SQL query...')
        logger.line(f'-------------------\n{query}\n-------------------')
        self.begin()
        try:
            result = conn.execute(query)
        except sa.exc.SQLAlchemyError as error:
            error_description = str(error.__dict__['orig'])
            self.save_as_failed(error_description=error_description)
            logger.info('SQL query failed')
            raise error
        else:
            result_value = result.rowcount
            self.save_as_completed(result_value=result_value)
            logger.info('SQL query completed')
            logger.info(f'{result_value} records inserted')
            return result_value

    def recycle(self, key_value):
        """Perform the recycle of this node."""
        conn = self.db.connect()
        table = self.proxy
        column = table.columns[self.key_field.label]
        delete = table.delete().where(column == key_value)
        conn.execute(delete)

    def prepare(self):
        """Prepare this node for the run."""
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
    def select(self):
        """Get formatted SQL select text."""
        return self.custom_query

    @select.setter
    def select(self, value):
        if isinstance(value, str):
            self.custom_query = value

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
    def text(self):
        """Get raw SQL text."""
        return self.assemble()

    @property
    def query(self):
        """Get formatted SQL text that can be executed in database."""
        return self.parse()

    def parse(self):
        """Parse final SQL statement."""
        query = self.assemble()
        query = self._format(query)
        query = self._hintinize(query)
        query = self.rewrite(query)
        return query

    def assemble(self):
        """Build raw SQL statement."""
        table = self.get_table()
        insert = table.insert()

        select = self.select
        columns = [sa.column(column) for column in self.describe()]

        if self.key_field and self.insert_key_field:
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

        select = sa.text(select).columns(*columns)
        insert = insert.from_select(columns, select)
        query = sql_compiler(insert, self.db)
        query = sql_formatter(query, fix_new_lines=True)
        return query

    def describe(self):
        """Get a real column list from the answerset."""
        conn = self.db.connect()
        query = self.select
        query = self._format(query)
        query = self._hintinize(query)
        query = sql_formatter(query, make_empty=True, db=self.db)
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

    def get_table(self):
        """Get object representing table."""
        return self.proxy

    def _format(self, text):
        text = text.format(**self.variables)
        return text

    def _hintinize(self, text):
        if self.db.vendor == 'oracle':
            statements = spa.parse(text)
            if self.parallel > 0:
                tvalue = 'SELECT'
                ttype = spa.tokens.Keyword.DML
                degree = '' if self.parallel is True else f'({self.parallel})'
                hint = f'/*+ parallel{degree} */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spa.sql.Token(ttype, tvalue)
                        statements[0].tokens[i] = new_token
            if self.append is True:
                tvalue = 'INSERT'
                ttype = spa.tokens.Keyword.DML
                hint = '/*+ append */'
                for i, token in enumerate(statements[0].tokens):
                    if token.match(ttype, tvalue):
                        tvalue = f'{tvalue} {hint}'
                        new_token = spa.sql.Token(ttype, tvalue)
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

    @property
    def path(self):
        """Get full file path."""
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str) or value is None:
            self._path = os.path.abspath(value) if value is not None else None

    @property
    def encoding(self):
        """Get file target encoding."""
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        if isinstance(value, str) or value is None:
            self._encoding = value

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

    def prepare(self):
        """Prepare model ETL operation."""
        if self.cleanup is True:
            logger.debug(f'File {self.path} will be completely purged')
            self.delete()

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

    @property
    def head(self):
        """Get header flag."""
        return self._head

    @head.setter
    def head(self, value):
        if isinstance(value, bool) or value is None:
            self._head = value

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

    @property
    def delimiter(self):
        """Get column delimiter."""
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        if isinstance(value, str) or value is None:
            self._delimiter = value

    @property
    def terminator(self):
        """Get line terminator."""
        return self._terminator

    @terminator.setter
    def terminator(self, value):
        if isinstance(value, str) or value is None:
            self._terminator = value

    @property
    def enclosure(self):
        """Get column enclosure."""
        return self._enclosure

    @enclosure.setter
    def enclosure(self, value):
        if isinstance(value, str) or value is None:
            self._enclosure = value

    @property
    def trim(self):
        """Get trim flag."""
        return self._trim

    @trim.setter
    def trim(self, value):
        if isinstance(value, bool) or value is None:
            self._trim = value

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

    def extract(self):
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

    def load(self, dataset):
        """Load data to CSV file."""
        with open(self.path, 'a', encoding=self.encoding, newline='') as fh:
            dialect = self.dialect
            fieldnames = [el for el in dataset[0]]
            writer = csv.DictWriter(fh, fieldnames, **dialect)
            if self.head is True and self.empty:
                writer.writeheader()
            writer.writerows(dataset)

    pass


class JSON(File):
    """Represents JSON file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8'):
        super().configure(file_name=file_name, path=path, encoding=encoding)

    def parse(self):
        """Parse JSON file and return its current content."""
        if not self.empty:
            with open(self.path, 'r', encoding=self.encoding) as fh:
                return json.load(fh)
        else:
            return []

    def extract(self):
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

    def load(self, dataset):
        """Load data to JSON file."""
        content = self.parse()
        with open(self.path, 'w', encoding=self.encoding) as fh:
            json.dump(content+dataset, fh, indent=2)

    pass


class XML(File):
    """Represents XML file as ETL model."""

    def configure(self, file_name=None, path=None, encoding='utf-8'):
        super().configure(file_name=file_name, path=path, encoding=encoding)

    def parse(self):
        """Parse XML file and/or get XML tree root."""
        if not self.empty:
            return xet.parse(self.path).getroot()
        else:
            return xet.Element('data')

    def extract(self):
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

    def load(self, dataset):
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


class Files(Model):
    """Represents file sequence as ETL model."""

    def configure(self, server_name=None, path=None, mask=None, recursive=None,
                  created=None, date_from=None, date_to=None):
        self.server_name = server_name
        self.path = path
        self.mask = mask
        self.recursive = recursive
        self.created = created or dict(date_from=date_from, date_to=date_to)

    @property
    def source_server(self):
        """Get source server proxy."""
        return self.file_server

    @property
    def server_name(self):
        """Get files source server name."""
        return self.source_name

    @server_name.setter
    def server_name(self, value):
        if isinstance(value, str):
            self.source_name = value

    @property
    def file_server(self):
        """Get files source server proxy."""
        return self.server

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

    def read(self):
        """Read full list of files."""
        for record in self.walk():
            path = record['path']
            logger.debug(f'File {path} found')
            yield record

    def filter(self):
        """Generate filtered list of files."""
        date_from, date_to = self.between()
        # logger.info(f'Using mask {self.mask} to filter files')
        logger.info('Using mask {mask} to filter files', mask=self.mask)
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
                    if os.path.exists(path):
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
                               root=self.path, dir=relpath, file=filename,
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
        values = calendar.Today()
        if self.pipeline and self.pipeline.calendar:
            values = self.pipeline.calendar
        minvalue = dt.datetime.min
        maxvalue = dt.datetime.max
        namespace = {'calendar': values}
        if isinstance(self.created, (dict, list, tuple)):
            if isinstance(self.created, dict):
                date_from = self.created.get('date_from')
                date_to = self.created.get('date_to')
            elif isinstance(self.created, (list, tuple)):
                date_from = self.created[0]
                date_to = self.created[1]
            date_from = eval(date_from or 'None', namespace) or minvalue
            date_to = eval(date_to or 'None', namespace) or maxvalue
        elif isinstance(self.created, str):
            date_from = eval(self.created, namespace)
            date_to = maxvalue
        elif self.date_field:
            date_from = self.date_from
            date_to = self.date_to
        else:
            date_from = minvalue
            date_to = maxvalue
        return (date_from, date_to)


class Filenames(Files, Extractable, Model):
    """Represents file names as ETL model."""

    def extract(self):
        """Extract data with file description."""
        yield from self.filter()

    pass


class FileManager(Files, Executable, Model):
    """Represents file manager as ETL model."""

    def configure(self, server_name=None, path=None, mask=None,
                  target_name=None, action='copy', destination=None,
                  recursive=False, nodirectory=False,
                  created=None, date_from=None, date_to=None,
                  zip=False, unzip=False, tempname=True):
        super().configure(server_name=server_name, path=path, mask=mask,
                          recursive=recursive, created=created,
                          date_from=date_from, date_to=date_to)
        self.target_name = target_name
        self.action = action
        self.destination = destination
        self.nodirectory = nodirectory
        self.zip = zip
        self.unzip = unzip
        self.tempname = tempname

    @property
    def target_name(self):
        """Get target server name."""
        return self._target_name

    @target_name.setter
    def target_name(self, value):
        if isinstance(value, str) or value is None:
            self._target_name = value.lower() if value else value
            if self._target_name == 'localhost' or value is None:
                self._target_server = Localhost()
            else:
                self._target_server = connector.receive(self._target_name)

    @property
    def target_server(self):
        """Get target connection proxy."""
        return self._target_server

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
    def destination(self):
        """Get destination file directories used for file processing."""
        return self._destination

    @destination.setter
    def destination(self, values):
        if isinstance(values, (str, list, tuple)) or values is None:
            if isinstance(values, str):
                self._destination = [values]
            elif isinstance(values, list):
                self._destination = values
            elif isinstance(values, tuple):
                self._destination = [value for value in values]
            elif values is None:
                self._destination = []

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

    def process(self, fileinfo):
        """Perform requested action for particular file."""
        path = fileinfo['path']
        logger.debug(f'File {path} processing...')
        try:
            # Source and target are local.
            if (
                isinstance(self.source_server, Localhost)
                and isinstance(self.target_server, Localhost)
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_localhost(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source and target are the same remote, SSH enabled.
            elif (
                self.source_server == self.target_server
                and self.file_server.ssh is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ssh(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ssh(fileinfo)
            # Source and target are the same remote, SFTP enabled.
            elif (
                self.source_server == self.target_server
                and self.file_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are the same remote, FTP enabled.
            elif (
                self.source_server == self.target_server
                and self.file_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_on_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source is local, target is remote, SFTP enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Localhost)
                and isinstance(self.target_server, Server)
                and self.target_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, SFTP enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server)
                and isinstance(self.target_server, Localhost)
                and self.source_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source is local, target is remote, FTP enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Localhost)
                and isinstance(self.target_server, Server)
                and self.target_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_localhost_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_localhost(fileinfo)
            # Source is remote, target is local, FTP enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server) is True
                and isinstance(self.target_server, Localhost) is True
                and self.source_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_localhost_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP on both enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server)
                and isinstance(self.target_server, Server)
                and self.source_server.sftp is True
                and self.target_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
            # Source and target are different remotes, FTP on both enabled.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server)
                and isinstance(self.target_server, Server)
                and self.source_server.ftp is True
                and self.target_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, FTP enabled on source,
            # SFTP enabled on target.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server)
                and isinstance(self.target_server, Server)
                and self.source_server.ftp is True
                and self.target_server.sftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_ftp_to_remote_by_sftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_ftp(fileinfo)
            # Source and target are different remotes, SFTP enabled on source,
            # FTP enabled on target.
            elif (
                self.source_server != self.target_server
                and isinstance(self.source_server, Server)
                and isinstance(self.target_server, Server)
                and self.source_server.sftp is True
                and self.target_server.ftp is True
            ):
                if self.action in ('copy', 'move'):
                    self._copy_from_remote_by_sftp_to_remote_by_ftp(fileinfo)
                if self.action in ('move', 'delete'):
                    self._delete_on_remote_by_sftp(fileinfo)
        except Exception:
            logger.warning()

    def execute(self, step):
        """Execute file manager action."""
        for fileinfo in self.filter():
            files_number = 1
            bytes_volume = fileinfo['size']
            step.files_read = files_number
            step.bytes_read = bytes_volume
            self.begin(fileinfo)
            self.process(fileinfo)
            self.finish()
            if self.action in ('copy', 'move'):
                step.files_written = files_number
                step.bytes_written = bytes_volume

    def _copy_on_localhost(self, fileinfo):
        for path in self.destination:
            if fileinfo['isfile'] is True:
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodirectory is False else '.'
                file = fileinfo['file']
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
                    with gzip.open(source, 'rb') as fhi:
                        with open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname else None
                    with open(source, 'rb') as fhi:
                        with gzip.open(temp or dest, 'wb') as fho:
                            fho.write(fhi.read())
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname else None
                shutil.copyfile(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_on_remote_by_ssh(self, fileinfo):
        conn = self.file_server.connect()
        for path in self.destination:
            if fileinfo['isfile'] is True:
                source = fileinfo['path']
                dir = fileinfo['dir'] if self.nodirectory is False else '.'
                file = fileinfo['file']
                dest = f'{path}/{dir}'
                conn.execute(f'mkdir -p {dest}')
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
                    conn.execute(f'gunzip -c {source} > {temp or dest}')
                    if temp is not None:
                        conn.execute(f'mv {temp} {dest}')
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname else None
                    conn.execute(f'gzip -c {source} > {temp or dest}')
                    if temp is not None:
                        conn.execute(f'mv {temp} {dest}')
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname else None
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
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
                    with gzip.open(temp or dest, 'wb') as fho:
                        conn.sftp.getfo(source, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname else None
                conn.sftp.get(source, temp or dest)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_localhost_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = conn.sftp.normalize(f'{path}/{dir}')
                if self.target_server.exists(dest) is False:
                    conn.sftp.mkdir(dest)
                dest = conn.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
                    with gzip.open(source, 'rb') as fhi:
                        conn.sftp.putfo(fhi, temp or dest)
                    if temp is not None:
                        conn.sftp.rename(temp, dest)
                    logger.info(f'File {source} unzipped to {dest}')
                    continue
                if self.zip == 'gz' or self.zip is True:
                    dest = f'{dest}.gz'
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
                conn.sftp.put(source, temp or dest)
                if temp is not None:
                    conn.sftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_remote_by_sftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn_in = self.source_server.connect()
            conn_out = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = conn_out.sftp.normalize(f'{path}/{dir}')
                if self.target_server.exists(dest) is False:
                    conn_out.sftp.mkdir(dest)
                dest = conn_out.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
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
            conn = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = f'{path}/{dir}'
                if self.target_server.exists(dest) is False:
                    conn.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
                with open(source, 'rb') as fhi:
                    conn.ftp.storbinary(f'STOR {temp or dest}', fhi)
                if temp is not None:
                    conn.ftp.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_localhost_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn = self.source_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = os.path.normpath(os.path.join(path, dir))
                if os.path.exists(dest) is False:
                    os.makedirs(dest)
                dest = os.path.join(dest, file)
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
                    with tempfile.TemporaryFile() as fhi:
                        conn.ftp.retrbinary(f'RETR {source}', fhi.write)
                        fhi.seek(0)
                        with gzip.open(temp or dest, 'wb') as fho:
                            shutil.copyfileobj(fhi, fho)
                    if temp is not None:
                        os.rename(temp, dest)
                    logger.info(f'File {source} zipped to {dest}')
                    continue
                temp = f'{dest}.tmp' if self.tempname else None
                with open(temp or dest, 'wb') as fho:
                    conn.ftp.retrbinary(f'RETR {source}', fho.write)
                if temp is not None:
                    os.rename(temp, dest)
                logger.info(f'File {source} copied to {dest}')

    def _copy_from_remote_to_remote_by_ftp(self, fileinfo):
        isfile = fileinfo['isfile']
        if isfile is True:
            conn_in = self.source_server.connect()
            conn_out = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = f'{path}/{dir}'
                if self.target_server.exists(dest) is False:
                    conn_out.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
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
            conn_in = self.source_server.connect()
            conn_out = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = conn_out.sftp.normalize(f'{path}/{dir}')
                if self.target_server.exists(dest) is False:
                    conn_out.sftp.mkdir(dest)
                dest = conn_out.sftp.normalize(f'{dest}/{file}')
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
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
            conn_in = self.source_server.connect()
            conn_out = self.target_server.connect()
            source = fileinfo['path']
            dir = fileinfo['dir'] if self.nodirectory is False else '.'
            file = fileinfo['file']
            for path in self.destination:
                dest = f'{path}/{dir}'
                if self.target_server.exists(dest) is False:
                    conn_out.ftp.mkd(dest)
                dest = f'{dest}/{file}'
                if self.unzip == 'gz' or self.unzip is True:
                    dest = os.path.splitext(dest)[0]
                    temp = f'{dest}.tmp' if self.tempname else None
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
                    temp = f'{dest}.tmp' if self.tempname else None
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
                temp = f'{dest}.tmp' if self.tempname else None
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
        conn = remote.connect()
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            conn.execute(f'rm -f {path}')
            logger.info(f'File {path} deleted')

    def _delete_on_remote_by_sftp(self, fileinfo):
        remote = fileinfo['server']
        conn = remote.connect()
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            conn.sftp.remove(path)
            logger.info(f'File {path} deleted')

    def _delete_on_remote_by_ftp(self, fileinfo):
        remote = fileinfo['server']
        conn = remote.connect()
        path = fileinfo['path']
        isfile = fileinfo['isfile']
        if isfile is True:
            conn.ftp.delete(path)
            logger.info(f'File {path} deleted')

    pass
