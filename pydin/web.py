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
from .utils import to_boolean


app = flask.Flask(__name__)
auth = flask_httpauth.HTTPTokenAuth(scheme='Bearer')
root = os.environ['PYDIN_HOME'] if os.environ.get('PYDIN_HOME') else locate()


OK = flask.Response(status=200)
BAD_REQUEST = flask.Response(status=400)
SERVER_ERROR = flask.Response(status=500)


class Server():
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

        self.server_name = platform.node()
        self.user_name = os.getlogin()
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

    @property
    def url(self):
        """Get server url."""
        return f'http://{self.host}:{self.port}'

    def start(self):
        """Start API server."""
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'RESTAPI')
        result = conn.execute(select).first()
        status = to_boolean(result.status)
        if not status:
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
            pid = proc.pid
            debug = 'X' if self.dev else None
            update = table.update().where(table.c.id == 'RESTAPI')
            update = update.values(status='Y', pid=pid, url=self.url,
                                   start_date=self.start_date,
                                   stop_date=self.stop_date,
                                   server_name=self.server_name,
                                   user_name=self.user_name,
                                   debug=debug)
            conn.execute(update)
        else:
            pid = result.pid
            print(f'Server already working on PID[{pid}]')

    def stop(self):
        """Stop API server."""
        self.status = False
        self.stop_date = dt.datetime.now()
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'RESTAPI')
        result = conn.execute(select).first()
        status = to_boolean(result.status)
        pid = result.pid
        if status and pid:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception as error:
                print(f'Server on PID[{pid}] stopped with error: {error}')
        update = table.update().where(table.c.id == 'RESTAPI')
        update = update.values(status='N', stop_date=self.stop_date)
        conn.execute(update)

    pass


@auth.verify_token
def verify_token(token):
    """."""
    TOKEN = config['API']['token']
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
    if id is not None and id.isdigit():
        driver = Driver(root=root)
        result = driver.enable_job(id)
        if result is True:
            return OK
        else:
            return SERVER_ERROR
    else:
        return BAD_REQUEST


@app.route('/api/disable-job', methods=['POST'])
@auth.login_required
def disable_job():
    """."""
    request = flask.request
    id = request.args.get('id')
    if id is not None and id.isdigit():
        driver = Driver(root=root)
        result = driver.disable_job(id)
        if result is True:
            return OK
        else:
            return SERVER_ERROR
    else:
        return BAD_REQUEST


@app.route('/api/run-job', methods=['POST'])
@auth.login_required
def run_job():
    """."""
    request = flask.request
    id = request.args.get('id')
    if id is not None and id.isdigit():
        process = request.args.get('process')
        date = request.args.get('date')
        recycle = json.loads(request.args.get('recycle', 'false'))
        wait = json.loads(request.args.get('wait', 'false'))
        kwargs = {}
        if process or date:
            k = 'process' if process else 'date'
            v = int(process) if process else dt.datetime.fromisoformat(date)
            kwargs[k] = v
        driver = Driver(root=root)
        proc = driver.run_job(id, recycle=recycle, wait=wait, **kwargs)
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


@app.route('/api/cancel-run', methods=['POST'])
@auth.login_required
def cancel_run():
    """."""
    request = flask.request
    process_id = request.args.get('id')
    if process_id and process_id.isdigit():
        process_id = int(process_id)
        driver = Driver(root=root)
        try:
            driver.cancel_run(process_id)
        except Exception:
            return BAD_REQUEST
        else:
            return OK


@app.route('/api/deactivate-run', methods=['POST'])
@auth.login_required
def deactivate_run():
    """."""
    request = flask.request
    process_id = request.args.get('id')
    if process_id and process_id.isdigit():
        process_id = int(process_id)
        driver = Driver(root=root)
        try:
            driver.deactivate_run(process_id)
        except Exception:
            return BAD_REQUEST
        else:
            return OK


@app.route('/api/sync-repo', methods=['POST'])
@auth.login_required
def sync_repo():
    """."""
    request = flask.request
    id = request.args.get('id')
    driver = Driver(root=root)
    driver.pull_repo()
    kwargs = {}
    if id is not None and id.isdigit():
        kwargs['id'] = int(id)
    driver.push_repo(**kwargs)
    return OK
