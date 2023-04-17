"""Contains main application configurator and other configuration elements."""

import os
import sys
import configparser


home = os.environ.get('PYDIN_HOME')
filename = 'pydin.ini'
user_config = os.path.abspath(os.path.expanduser(f'~/.pydin/{filename}'))
home_config = os.path.join(home, filename) if home is not None else None
local_config = os.path.join(os.path.dirname(sys.argv[0]), filename)


class Configurator(dict):
    """Represents main application configurator."""

    def __init__(self):
        super().__init__()
        tables = ['GENERAL', 'DATABASE', 'SMTP', 'API',
                  'ENVIRONMENTS', 'SCHEDULER', 'JOB',
                  'LOGGING', 'EMAIL']
        for table in tables:
            self[table] = self.Table(name=table)

    class Table(dict):
        """Represents configuration table."""

        def __init__(self, name, **kwargs):
            self.name = name
            super().__init__(**kwargs)

        def __setitem__(self, key, value):
            """Normalize given item key and set item value."""
            super().__setitem__(key.lower(), value)

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

    def save(self):
        """Save current settings to the user configuration file."""
        config_parser = configparser.ConfigParser()
        config_dict = {'GENERAL': self['GENERAL'],
                       'DATABASE': self['DATABASE'],
                       'EMAIL': self['EMAIL'],
                       'API': self['API'],
                       'ENVIRONMENTS': self['ENVIRONMENTS']}
        config_parser.read_dict(config_dict)
        with open(user_config, 'w', encoding='utf8') as fh:
            config_parser.write(fh, space_around_delimiters=False)

    def has_table(self, table):
        """Check if configuration table exists."""
        if self.get(table) is not None:
            return True
        else:
            return False


###############################################################################
#                      Module level configuration objects
###############################################################################
config = Configurator()
config.load([user_config, home_config, local_config])


###############################################################################
#                              Default parameters
###############################################################################
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

DATABASE_VENDOR_NAME = None
DATABASE_DRIVER_NAME = None
DATABASE_PATH = None
DATABASE_HOST = None
DATABASE_PORT = None
DATABASE_SID = None
DATABASE_SERVICE_NAME = None
DATABASE_USERNAME = None
DATABASE_PASSWORD = None

EMAIL_TOGGLE = False
EMAIL_HOST = None
EMAIL_PORT = None
EMAIL_TLS = False
EMAIL_ADDRESS = None
EMAIL_USERNAME = None
EMAIL_PASSWORD = None

API_HOST = None
API_PORT = None
API_TOKEN = None

EDITOR = None
OWNER = None

SCHEDULER_NAME = None
SCHEDULER_DESC = None
SCHEDULER_CHARGERS_NUMBER = 5
SCHEDULER_EXECUTORS_NUMBER = 20
SCHEDULER_REFRESH_INTERVAL = 300
SCHEDULER_RERUN_DELAY = 14400
SCHEDULER_RERUN_ENABLED = True
SCHEDULER_RERUN_INTERVAL = 60
SCHEDULER_WAKEUP_INTERVAL = 60
SCHEDULER_WAKEUP_ENABLED = True

ENV_PYTHON = sys.executable
