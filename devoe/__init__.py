"""Devoe.

Python ETL framework.
"""

from .task import Task
from .nodes import (Database, connect_to_database,
                    Remote, Localhost, connect_to_server)
from .items import (Item, Table, Insert, Merge, Select, TeradataTable, CsvFile,
                    JsonFile, XmlFile, Asn1File, SqlScript, TeradataScript,
                    TeradataFastload, BashScript, Filenames, FileManager,
                    FileMerge, Log, Email, DoNothing)
from .pipeline import Pipeline


__author__ = 'Timur Faradzhov'
__copyright__ = 'Copyright 2019, The Devoe Project'
__credits__ = ['Timur Faradzhov']

__license__ = 'MIT'
__version__ = '0.1.0'
__maintainer__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__status__ = 'Development'
__doc__ = 'Python ETL framework.'
