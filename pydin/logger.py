"""Contains main application logger."""

import pepperoni as pe

from .config import config


logger = pe.logger('pydin.core.logger',
                   format='{isodate}  {thread}  {rectype}  {message}\n',
                   console=config['LOGGING'].get('console'),
                   file=config['LOGGING'].get('file'),
                   info=config['LOGGING'].get('info'),
                   debug=config['LOGGING'].get('debug'),
                   error=config['LOGGING'].get('error'),
                   warning=config['LOGGING'].get('warning'),
                   critical=config['LOGGING'].get('critical'),
                   alarming=config['LOGGING'].get('alarming'),
                   maxsize=config['LOGGING'].get('maxsize'),
                   maxdays=config['LOGGING'].get('maxdays'),
                   email=config['EMAIL'].get('toggle'),
                   smtp={'host': config['EMAIL'].get('host'),
                         'port': config['EMAIL'].get('port'),
                         'tls': config['EMAIL'].get('tls'),
                         'address': config['EMAIL'].get('address'),
                         'user': config['EMAIL'].get('username'),
                         'password': config['EMAIL'].get('password'),
                         'recipients': config['GENERAL'].get('owner')})
