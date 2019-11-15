import ftplib
import os
import paramiko

from ...config import make_config
from ...log import make_log
from ...share import connections


userconfig = os.path.abspath(os.path.expanduser('~/.devoe/host.json'))


def connect_to_server(name):
    if connections.get(name) is not None:
        return connections.get(name)
    else:
        config = make_config(obj=userconfig).get(name)
        if config is None:
            raise KeyError(f'unknown connection {name}')
        else:
            host = config.get('host')
            port = config.get('port')
            user = config.get('user')
            password = config.get('password')
            key = config.get('key')
            keyfile = config.get('keyfile')
            is_ssh = config.get('is_ssh')
            is_sftp = config.get('is_sftp')
            is_ftp = config.get('is_ftp')
            host = Remote(name=name, host=host, port=port, user=user,
                          password=password, key=key, keyfile=keyfile,
                          is_ssh=is_ssh, is_sftp=is_sftp, is_ftp=is_ftp)
            return host

make_remote = connect_to_server

class Localhost():

    def __str__(self):
        log = make_log()
        return log.sysinfo.stat.hostname

    __repr__ = __str__

class Remote():

    def __init__(self, name=None, host=None, port=None, user=None,
                 password=None, key=None, keyfile=None, is_ssh=False,
                 is_sftp=False, is_ftp=False, config=None):
        self.log = make_log()
        self.name = None
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.key = None
        self.keyfile = None
        self.is_ssh = None
        self._ssh = None
        self.is_sftp = None
        self._sftp = None
        self.is_ftp = None
        self._ftp = None

        self.configure(name=name, host=host, port=port, user=user,
                       password=password, key=key, keyfile=keyfile,
                       is_ssh=is_ssh, is_sftp=is_sftp, is_ftp=is_ftp,
                       config=config)
        pass

    def __str__(self):
        return self.name

    __repr__ = __str__

    @property
    def ssh(self):
        return self._ssh

    @property
    def sftp(self):
        return self._sftp

    @property
    def ftp(self):
        return self._ftp

    def configure(self, name=None, host=None, port=None, user=None,
                  password=None, key=None, keyfile=None, is_ssh=None,
                  is_sftp=None, is_ftp=None, config=None):
        if config is not None:
            config = make_config(obj=config)
            name = config.get('name', name)
            host = config.get('host', host)
            port = config.get('port', port)
            user = config.get('user', user)
            password = config.get('password', password)
            key = config.get('key', key)
            keyfile = config.get('keyfile', keyfile)
            is_ssh = config.get('is_ssh', is_ssh)
            is_sftp = config.get('is_sftp', is_sftp)
            is_ftp = config.get('is_ftp', is_ftp)

        if isinstance(name, str) is True: self.name = name
        if isinstance(host, str) is True: self.host = host
        if isinstance(port, int) is True: self.port = port
        if isinstance(user, str) is True: self.user = user
        if isinstance(key, str) is True: self.key = key
        if isinstance(keyfile, str) is True: self.keyfile = keyfile
        if isinstance(is_ssh, bool) is True: self.is_ssh = is_ssh
        if isinstance(is_sftp, bool) is True: self.is_sftp = is_sftp
        if isinstance(is_ftp, bool) is True: self.is_ftp = is_ftp

        if (password is not None or key is not None or keyfile is not None) \
        and (host is not None or port is not None or user is not None):
            self.connect(password)
        pass

    def connect(self, password):
        try:
            if self.is_ssh is True:
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self._ssh.connect(self.host, port=self.port,
                                  username=self.user, password=password,
                                  key_filename=self.keyfile)
            if self.is_sftp is True:
                if self.is_ssh is True:
                    self._sftp = self.ssh.open_sftp()
                else:
                    transport = paramiko.Transport((self.host, self.port))
                    if self.keyfile is not None:
                        key = paramiko.RSAKey(filename=self.keyfile)
                        transport.connect(username=self.user, pkey=key)
                    else:
                        transport.connect(username=self.user, password=password)
                    self._sftp = paramiko.SFTPClient.from_transport(transport)
            if self.is_ftp is True:
                self._ftp = ftplib.FTP(host=self.host, user=self.user,
                                       passwd=password)
            # Share connections.
            connections[self.name] = self
        except:
            self.log.info('Cannot connect to host')
            self.log.critical()
        pass

    def execute(self, command, **kwargs):
        if self.is_ssh is True:
            stdin, stdout, stderr = self.ssh.exec_command(command, **kwargs)
            stdout = stdout.read().decode()
            stderr = stderr.read().decode()
            if stderr: raise paramiko.ssh_exception.SSHException(stderr)
            return stdout, stderr

    def exists(self, path):
        if self.is_sftp is True:
            try:
                self.sftp.chdir(path)
                return True
            except IOError:
                return False
        elif self.is_ftp is True:
            try:
                self.ftp.cwd(path)
                return True
            except:
                return False
