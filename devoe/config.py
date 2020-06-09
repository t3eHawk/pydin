"""Contains main application configurator with core parameters."""

import configparser
import os
import sys


home = os.environ.get('DEVOE_HOME')
filename = 'devoe.ini'
user_config = os.path.abspath(os.path.expanduser(f'~/.devoe/{filename}'))
home_config = os.path.join(home, filename) if home is not None else None
local_config = os.path.join(os.path.dirname(sys.argv[0]), filename)


class Configurator(dict):
    """Represents application configurator."""

    def __init__(self, **kwargs):
        tables = ['GENERAL', 'DATABASE', 'SMTP', 'API',
                  'ENVIRONMENTS', 'SCHEDULER', 'JOB',
                  'LOGGING', 'EMAIL']
        for table in tables:
            kwargs[table] = self.Table(name=table)
        super().__init__(**kwargs)
        pass

    class Table(dict):
        """Represents configuration table."""

        def __init__(self, name, **kwargs):
            self.name = name
            super().__init__(**kwargs)
            pass

        def __setitem__(self, key, value):
            """Normalize given item key and set item value."""
            super().__setitem__(key.lower(), value)
            pass

        def __getitem__(self, key):
            """Normalize given item key and get item value if it exists."""
            return super().__getitem__(key.lower())

        def get(self, key):
            """Normalize given item key and get item value or return None."""
            return super().get(key.lower())

        def has_record(self, record):
            """Check if configuration table record exists."""
            if self.get(record) is not None:
                return True
            else:
                return False
            pass

        pass

    def load(self, paths, encoding=None):
        """Load extetrnal configuration from files to configurator."""
        paths = [path for path in paths if path is not None]
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(paths, encoding=encoding)
        for section in parser.sections():
            for option in parser.options(section):
                value = parser[section][option]
                if (
                    value is None
                    or value.upper() == 'NONE'
                    or value.isspace() is True
                   ):
                    self[section][option] = None
                elif value.upper() == 'TRUE':
                    self[section][option] = True
                elif value.upper() == 'FALSE':
                    self[section][option] = False
                elif value.isdigit() is True:
                    self[section][option] = int(value)
                elif value.isdecimal() is True:
                    self[section][option] = float(value)
                else:
                    self[section][option] = value
        return self

    def has_table(self, table):
        """Check if configuration table exists."""
        if self.get(table) is not None:
            return True
        else:
            return False
        pass


config = Configurator()
config.load([user_config, home_config, local_config])

DEBUG = False

LOG_CONSOLE = True
LOG_FILE = True
LOG_INFO = True
LOG_DEBUG = False
LOG_ERROR = True
LOG_WARNING = True
LOG_CRITICAL = True
LOG_ALARMING = False
LOG_MAXSIZE = 10485760
LOG_MAXDAYS = 1

DATABASE_VENDOR = None
DATABASE_DRIVER = None
DATABASE_PATH = None
DATABASE_HOST = None
DATABASE_PORT = None
DATABASE_SID = None
DATABASE_SERVICE = None
DATABASE_USER = None
DATABASE_PASSWORD = None

EMAIL_TOGGLE = False
EMAIL_HOST = None
EMAIL_PORT = None
EMAIL_TLS = False
EMAIL_ADDRESS = None
EMAIL_USER = None
EMAIL_PASSWORD = None

API_HOST = None
API_PORT = None
API_TOKEN = None

EDITOR = None
OWNER = None

SCHEDULER_NAME = None
SCHEDULER_DESC = None
SCHEDULER_CHARGERS = 5
SCHEDULER_EXECUTORS = 20
SCHEDULER_RESCHEDULE = 60
SCHEDULER_RERUN = 3600

ENV_PYTHON = sys.executable
