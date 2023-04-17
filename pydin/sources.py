"""Contains data source interfaces."""

import platform
import importlib as imp
import ftplib

import pepperoni as pe

import paramiko as pm
# import sqlalchemy as sa


class Localhost():
    """Represents localhost i.e. no connection required."""

    def __init__(self):
        self.host = platform.node()


class Server():
    """Represents ordinary remote server."""

    def __init__(self, name=None, host=None, port=None, user=None,
                 password=None, key=None, keyfile=None,
                 ssh=False, sftp=False, ftp=False):
        self.name = name.lower() if isinstance(name, str) else None
        self.host = host if isinstance(host, str) else None
        self.port = int(port) if isinstance(port, (str, int)) else None

        creds = {}
        creds['user'] = user if isinstance(user, str) else None
        creds['password'] = password if isinstance(password, str) else None
        creds['key'] = key if isinstance(key, str) else None
        creds['keyfile'] = keyfile if isinstance(keyfile, str) else None
        self.creds = creds

        self.ssh = ssh if isinstance(ssh, bool) else False
        self.sftp = sftp if isinstance(sftp, bool) else False
        self.ftp = ftp if isinstance(ftp, bool) else False

        cache = imp.import_module('pydin.cache')
        cache.CONNECTIONS[f'{self.name}'] = self

    def __repr__(self):
        """Represent server as its name."""
        return self.name

    class Connection():
        """Represents ordinary remote server connection."""

        def __init__(self, server, creds):
            server = server if isinstance(server, Server) else None
            user = creds.get('user')
            password = creds.get('password')
            keyfile = creds.get('keyfile')
            if server.ssh is True and server is not None:
                self.ssh = pm.SSHClient()
                self.ssh.set_missing_host_key_policy(pm.AutoAddPolicy())
                self.ssh.connect(server.host, port=server.port,
                                 username=user, password=password,
                                 key_filename=keyfile)
            if server.sftp is True and server is not None:
                if server.ssh is True:
                    self.sftp = self.ssh.open_sftp()
                else:
                    transport = pm.Transport((server.host, server.port))
                    if keyfile is not None:
                        key = pm.RSAKey(filename=keyfile)
                        transport.connect(username=user, pkey=key)
                    else:
                        transport.connect(username=user,
                                          password=password)
                    self.sftp = pm.SFTPClient.from_transport(transport)
            if server.ftp is True and server is not None:
                self.ftp = ftplib.FTP(host=server.host,
                                      user=user,
                                      passwd=password)

        def execute(self, command, **kwargs):
            """Execute given command in server terminal."""
            result = self.ssh.exec_command(command, **kwargs)
            stdout = result[1].read().decode()
            stderr = result[2].read().decode()
            return stdout, stderr

    @property
    def connection(self):
        """Get active server connection."""
        if hasattr(self, '_connection'):
            return self._connection
        return None

    @connection.setter
    def connection(self, value):
        if isinstance(value, self.Connection):
            self._connection = value

    def connect(self):
        """Connect to remote server."""
        if not self.connection:
            self.connection = self.Connection(self, self.creds)
        return self.connection

    def exists(self, path):
        """Check if file with given path exists in file system."""
        conn = self.connect()
        if conn:
            if self.sftp:
                try:
                    conn.sftp.chdir(path)
                    return True
                except IOError:
                    return False
            elif self.ftp:
                try:
                    conn.ftp.cwd(path)
                    return True
                except Exception:
                    return False
        else:
            raise ConnectionError(f'not connected to {self.name}')


class Database(pe.Database):
    """Represents database server."""

    def __init__(self, name=None, vendor_name=None, driver_name=None,
                 path=None, host=None, port=None, sid=None, service_name=None,
                 user=None, password=None):
        self.name = name.lower() if isinstance(name, str) else None
        super().__init__(vendor=vendor_name, driver=driver_name,
                         path=path, host=host, port=port,
                         sid=sid, service=service_name,
                         user=user, password=password)

        cache = imp.import_module('pydin.cache')
        cache.CONNECTIONS[f'{self.name}'] = self

    def __repr__(self):
        """Represent database as its name."""
        return self.name
