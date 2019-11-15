import json
import os
import sqlalchemy as sql

from .convs import naming_convention
from .utils import parse_config
from ...config import make_config
from ...log import make_log
from ...share import connections


userconfig = os.path.abspath(os.path.expanduser('~/.devoe/db.json'))


def connect_to_database(name):
    if connections.get(name) is not None:
        return connections.get(name)
    else:
        config = make_config(obj=userconfig).get(name)
        if config is None:
            raise KeyError(f'unknown connection {name}')
        else:
            vendor = config.get('vendor')
            host = config.get('host')
            port = config.get('port')
            sid = config.get('sid')
            user = config.get('user')
            password = config.get('password')
            database = Database(name=name, vendor=vendor, host=host, port=port,
                                sid=sid, user=user, password=password)
            return database

make_database = connect_to_database

class Database():

    def __init__(self, name=None, vendor=None, host=None, port=None, sid=None,
                 user=None, password=None, credentials=None, config=None):
        self.log = make_log()
        self.name = None
        self.vendor = None
        self.host = None
        self.port = None
        self.sid = None
        self.user = None
        self.schema = None
        self._engine = None
        self._metadata = None
        self._connection = None
        self.compargs = None

        self.configure(name=name, vendor=vendor, host=host, port=port, sid=sid,
                       user=user, password=password, credentials=credentials,
                       config=config)
        pass

    @property
    def engine(self):
        return self._engine

    @property
    def metadata(self):
        return self._metadata

    @property
    def connection(self):
        return self._connection

    def configure(self, name=None, vendor=None, host=None, port=None, sid=None,
                  user=None, password=None, config=None, credentials=None):
        if config is not None:
            config = make_config(obj=config)
            name = config.get('name', name)
            vendor = config.get('vendor', vendor)
            host = config.get('host', host)
            port = config.get('port', port)
            sid = config.get('sid', sid)
            user = config.get('user', user)
            password = config.get('password', password)

        if isinstance(name, str) is True:
            self.name = name.lower()
        if isinstance(vendor, str) is True:
            self.vendor = vendor.lower()
        if isinstance(host, str) is True:
            self.host = host
        if isinstance(port, (int, str)) is True:
            self.port = port
        if isinstance(sid, str) is True:
            self.sid = sid
        if isinstance(user, str) is True:
            self.user = self.schema = user.lower()

        if isinstance(credentials, str) is False:
            login = f'{self.user}:{password}'
            address = f'{self.host}:{self.port}/{self.sid}'
            credentials = f'{self.vendor}://{login}@{address}'

        if isinstance(password, str) is True and (
            host is not None or port is not None or sid is not None \
            or user is not None
        ):
            self.connect(credentials)

        self.compargs = {
            'bind': self.engine, 'compile_kwargs': {'literal_binds': True}}
        pass

    def connect(self, credentials):
        try:
            self._engine = sql.create_engine(credentials)
            self._metadata = sql.MetaData(naming_convention=naming_convention)
            self.log.debug('Connecting to database...')
            self._connection = self.engine.connect()
            # Share connections.
            connections[self.name] = self
        except:
            self.log.critical()
        pass

    def inspect(self, path=None, tech=False):
        """Get the structure of database objects."""
        inspector = sql.engine.reflection.Inspector.from_engine(self.engine)

        schemas = inspector.get_schema_names()
        data = {}
        schemas_ = {}
        for schema in schemas:
            schema_ = {}

            tables_ = {}
            tables = inspector.get_table_names(schema)
            for table in tables:
                table_ = {}

                if '$' in table and tech is False:
                    continue

                columns_ = []
                columns = inspector.get_columns(table, schema)
                for column in columns:
                    try:
                        name = column['name']
                        type = column['type']
                        column_ = f'{name} ({type})'
                        columns_.append(column_)
                    except:
                        continue

                table_['columns'] = columns_

                primary_key_ = {}
                primary_key = inspector.get_pk_constraint(table, schema)
                primary_key_name = primary_key['name']
                primary_key_columns = primary_key['constrained_columns']
                if primary_key_name is not None and primary_key_columns:
                    primary_key_[primary_key_name] = primary_key_columns
                table_['primary_key'] = primary_key_

                indexes_ = {}
                indexes = inspector.get_indexes(table, schema)
                for index in indexes:
                    name = index['name']
                    columns = index['column_names']
                    indexes_[name] = columns
                table_['indexes'] = indexes_

                foreign_keys_ = {}
                foreign_keys = inspector.get_foreign_keys(table, schema)
                for foreign_key in foreign_keys:
                    foreign_key_ = {}

                    name = foreign_key['name']
                    columns = foreign_key['constrained_columns']
                    referred_schema = foreign_key['referred_schema']
                    referred_table = foreign_key['referred_table']
                    referred_columns = foreign_key['referred_columns']

                    foreign_key_['columns'] = columns
                    foreign_key_['referred_schema'] = referred_schema
                    foreign_key_['referred_table'] = referred_table
                    foreign_key_['referred_columns'] = referred_columns

                    foreign_keys_[name] = foreign_key_

                table_['foreign_keys'] = foreign_keys_

                tables_[table] = table_

            schema_['tables'] = tables_

            views_ = []
            views = inspector.get_view_names(schema)
            for view in views:
                views_.append(view)
            schema_['views'] = views_

            schemas_[schema] = schema_

        data['schemas'] = schemas_

        if path is not None:
            with open(path, "w") as fp:
                json.dump(data, fp, indent=2, sort_keys=True)
        return data
