"""Python ETL framework."""

from .api import Driver
from .cli import Manager
from .web import Server
from .core import Scheduler, Job, Pipeline

from .models import Mapper
from .models import Table, SQL, Select, Insert
from .models import CSV, JSON, XML
from .models import Filenames, FileManager

from .logger import logger
from .fields import run_id, task_id, step_id, process_id
from .utils import get_version, get_job, get_logger, get_email, get_credentials


__version__ = '0.1.12'
__status__ = 'Development'

__author__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2022, The PyDin Project'
__maintainer__ = __author__

__all__ = [Driver, Manager, Server, Scheduler, Job, Pipeline, Mapper,
           Table, SQL, Select, Insert, CSV, JSON, XML, Filenames, FileManager,
           logger, run_id, task_id, step_id, process_id,
           get_version, get_job, get_logger, get_email, get_credentials]
