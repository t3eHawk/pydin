"""Python ETL framework."""

from .cli import Manager
from .core import Scheduler, Job, Task, Pipeline
from .web import API

from .api import Operator
from .items import Mapper
from .items import Table, SQL, Select, Insert
from .items import CSV, JSON, XML
from .items import Files, FileManager

__version__ = '0.0.2'
__status__ = 'Development'

__author__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2022, The Devoe Project'
__maintainer__ = __author__

__all__ = [Manager, Operator, Scheduler, Job, Task, Pipeline, API,
           Mapper, Table, SQL, Select, Insert, CSV, JSON, XML, Files,
           FileManager]
