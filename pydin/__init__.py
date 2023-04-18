"""Python ETL framework."""

from .api import Driver
from .cli import Manager
from .web import Server
from .core import Scheduler, Job, Pipeline

from .models import Mapper
from .models import Table, SQL, Select, Insert
from .models import CSV, JSON, XML
from .models import Filenames, FileManager

from .models import Model
from .models import Extractable, Transformable, Loadable, Executable

from .logger import logger
from .utils import Logging, Calendar
from .utils import calendar, connector
from .utils import get_version, get_system_information
from .utils import get_job, get_logger, get_email, get_credentials

from .fields import run_id, task_id, step_id, process_id

__version__ = '0.1.15'
__status__ = 'Production'

__author__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2023, The PyDin Project'
__maintainer__ = __author__

__all__ = [Driver, Manager, Server, Scheduler, Job, Pipeline, Mapper,
           Model, Extractable, Transformable, Loadable, Executable,
           Table, SQL, Select, Insert, CSV, JSON, XML, Filenames, FileManager,
           Logging, Calendar, logger, calendar, connector,
           get_version, get_system_information, get_logger, get_email,
           get_credentials, get_job, run_id, task_id, step_id, process_id]
