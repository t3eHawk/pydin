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

