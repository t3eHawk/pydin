"""Contains web API server."""

import datetime as dt
import json
import os
import platform
import signal
import subprocess as sp
import sys

import flask
import flask_httpauth

from .api import Driver

from .db import db
from .config import config
from .logger import logger

from .utils import locate
from .utils import to_process


app = flask.Flask(__name__)
auth = flask_httpauth.HTTPTokenAuth(scheme='Bearer')
root = os.environ['PYDIN_HOME'] if os.environ.get('PYDIN_HOME') else locate()
TOKEN = config['API']['token']


OK = flask.Response(status=200)
BAD_REQUEST = flask.Response(status=400)
SERVER_ERROR = flask.Response(status=500)


class Interface():
    """Represents web API server."""

    def __init__(self, host=None, port=None, dev=False):
        self.app = app
        argv = sys.argv.copy()

        self.host = host or '0.0.0.0'
        self.port = port or 8080
        if config['API'].get('host'):
            self.host = config['API'].get('host')
        if config['API'].get('host'):
            self.port = config['API'].get('port')

        self.server = platform.node()
        self.user = os.getlogin()
        self.start_date = None
        self.stop_date = None
        self.status = None

        dev = True if 'dev' in argv else dev
        self.dev = dev if isinstance(dev, bool) is True else False

        logger.configure(file=False, format='[{rectype}] {message}\n',
                         alarming=False)

        if len(argv) > 1:
            if argv[1] == 'start':
                self.start()
            elif argv[1] == 'stop':
                self.stop()
        pass

    def start(self):
        """Start API server."""
        self.status = True
        self.start_date = dt.datetime.now()
        app = 'pydin.web:app'
        env = os.environ.copy()
        env['PYDIN_HOME'] = locate()
        if self.dev is True:
            script = ['flask', 'run']
            args = ['--host', self.host, '--port', str(self.port)]
            cmd = [arg for arg in [*script, *args] if arg is not None]
            mode = 'development'
            env['FLASK_APP'] = app
            env['FLASK_ENV'] = mode
            try:
                proc = sp.Popen(cmd, env=env)
                proc.wait()
            except KeyboardInterrupt:
                proc.terminate()
        else:
            script = 'waitress-serve'
            args = ['--host', self.host, '--port', str(self.port), app]
            args = [arg for arg in args if arg is not None]
            proc = to_process(script, args=args, env=env, devnull=True)

            conn = db.connect()
            table = db.tables.components
            update = table.update().where(table.c.id == 'RESTAPI')
            pid = proc.pid
            update = update.values(status='Y', pid=pid,
                                   start_date=self.start_date,
                                   stop_date=self.stop_date,
                                   server=self.server, user=self.user)
            conn.execute(update)
        pass

    def stop(self):
        """Stop API server."""
        self.status = False
        self.stop_date = dt.datetime.now()
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'RESTAPI')
        update = table.update().where(table.c.id == 'RESTAPI')
        result = conn.execute(select).first()
        status = True if result.status == 'Y' else False
        pid = result.pid
        if status is True and pid is not None:
            try:
                os.kill(pid, signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass
            update = update.values(status='N', stop_date=self.stop_date)
            conn.execute(update)
        pass


@auth.verify_token
def verify_token(token):
    """."""
    if token == TOKEN:
        return token


@app.route('/api/test')
@auth.login_required
def test():
    """Test server working capacity with a simple request."""
    return 'Hello there!'


@app.route('/api/help')
@auth.login_required
def help():
    """."""
    demo = os.path.join(os.path.dirname(__file__), 'demo/web.txt')
    lines = open(demo, 'r').readlines()
    text = ''.join(lines)
    return text


@app.route('/api/start-scheduler', methods=['POST'])
@auth.login_required
def start_scheduler():
    """."""
    driver = Driver(root=root)
    pid = driver.start_scheduler()
    if pid is None:
        return SERVER_ERROR
    else:
        return OK
    pass


@app.route('/api/stop-scheduler', methods=['POST'])
@auth.login_required
def stop_scheduler():
    """."""
    driver = Driver(root=root)
    driver.stop_scheduler()
    return OK


@app.route('/api/create-job', methods=['POST'])
@auth.login_required
def create_job():
    """."""
    driver = Driver(root=root)
    driver.create_job()
    return OK


@app.route('/api/enable-job', methods=['POST'])
@auth.login_required
def enable_job():
    """."""
    request = flask.request
    id = request.args.get('id')
    if id is not None and id.isdigit() is True:
        driver = Driver(root=root)
        result = driver.enable_job(id)
        if result is True:
            return OK
        else:
            return SERVER_ERROR
    else:
        return BAD_REQUEST
    pass


@app.route('/api/disable-job', methods=['POST'])
@auth.login_required
def disable_job():
    """."""
    request = flask.request
    id = request.args.get('id')
    if id is not None and id.isdigit() is True:
        driver = Driver(root=root)
        result = driver.disable_job(id)
        if result is True:
            return OK
        else:
            return SERVER_ERROR
    else:
        return BAD_REQUEST
    pass


@app.route('/api/run-job', methods=['POST'])
@auth.login_required
def run_job():
    """."""
    request = flask.request
    id = request.args.get('id')
    if id is not None and id.isdigit() is True:
        run = request.args.get('run')
        date = request.args.get('date')
        wait = json.loads(request.args.get('wait', 'false'))
        kwargs = {}
        if run is not None or date is not None:
            if run is not None:
                key = 'run'
                value = int(run)
            elif date is not None:
                key = 'date'
                value = dt.datetime.fromisoformat(date)
            kwargs[key] = value
        driver = Driver(root=root)
        proc = driver.run_job(id, wait=False, **kwargs)
        if wait is True:
            proc.wait()
            if proc.returncode > 0:
                return SERVER_ERROR
            else:
                return OK
        else:
            return OK
    else:
        return BAD_REQUEST
    pass


@app.route('/api/sync-repo', methods=['POST'])
@auth.login_required
def sync_repo():
    """."""
    request = flask.request
    id = request.args.get('id')
    driver = Driver(root=root)
    driver.pull_repo()
    kwargs = {}
    if id is not None and id.isdigit() is True:
        kwargs['id'] = int(id)
    driver.push_repo(**kwargs)
    return OK
