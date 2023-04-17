"""Contains application manager."""

import argparse
import getpass
import time
import datetime as dt
import os
import re
import subprocess as sp
import sys

from .api import Driver

from .db import db
from .config import config
from .logger import logger
from .utils import connector
from .utils import locate
from .utils import to_none


class Manager():
    """Represents application manager which is a common CLI."""

    def __init__(self):
        self.root = locate()
        self.driver = Driver()

        args = self._parse_arguments()
        self.sure = args.sure
        self.wait = args.wait
        self.output = args.output
        self.error = args.error

        logger.configure(status=False, file=False, console=False,
                         format='[{rectype}] {message}\n',
                         alarming=False)

        # If argv more than one then command was entered.
        command = [arg for arg in sys.argv if arg.startswith('-') is False]
        self.parse(command)

    def parse(self, command):
        """Parse and execute command."""
        if len(command) == 1:
            self.start_console()
        elif len(command) == 2 and command[1] == 'help':
            self.help()
        elif len(command) == 2 and command[1] == 'install':
            self.install()
        elif len(command) > 2 and command[-1] == 'help':
            name = '_'.join(command[1:2 if len(command) == 3 else 3])
            self.help(name=name)
        elif len(command) > 2:
            # Read command. Command length is two words as maximum.
            try:
                name = '_'.join(command[1:3])
                args = command[3:] if len(command) > 3 else []
                func = getattr(self, name)
            except AttributeError:
                self.unknown()
            else:
                try:
                    func(*args)
                except TypeError as e:
                    if (
                        e.args[0].startswith(f'{name}() takes')
                        or e.args[0].startswith(f'{name}() missing')
                       ):
                        self.incorrect(name)
                    else:
                        logger.error()
                except Exception:
                    logger.error()
        else:
            self.unknown()

    def help(self, name=None):
        """Show help note for all available commands."""
        if name == 'help' or name is None:
            funcs = ['start_console', 'install',
                     'create_scheduler',
                     'start_scheduler', 'stop_scheduler',
                     'restart_scheduler',
                     'create_job', 'edit_script',
                     'configure_job',
                     'enable_job', 'disable_job', 'delete_job',
                     'list_jobs',
                     'run_job', 'run_jobs', 'cancel_job', 'cancel_jobs',
                     'cancel_run', 'deactivate_run',
                     'create_config', 'edit_config',
                     'create_repo', 'sync_repo']
            docs = {}
            for func in funcs:
                doc = getattr(self, func).__doc__.splitlines()[0]
                docs[func] = doc
            demo = os.path.join(os.path.dirname(__file__), 'demo/cli.txt')
            text = open(demo, 'r').read().format(**docs)
            print(text)
        elif name in dir(self):
            func = getattr(self, name)
            text = func.__doc__
            print(text)
        else:
            self.unknown()

    def incorrect(self, name):
        """Print message about incorrect command."""
        name = name.replace('_', ' ')
        print(f'Incorrect command. Type *{name} help* to see details.')

    def unknown(self):
        """Print message about unknown command."""
        print('Unknown command. Type *help* to get list of commands.')

    def start_console(self):
        """Start interactive console."""
        print('Hello there!')
        while True:
            now = dt.datetime.now()
            command = input(f'{now:%H:%M} $ ')
            if command == 'quit':
                break
            elif command == '':
                continue
            else:
                command = ['manager.py', *command.split()]
                self.parse(command)
        print('Good bye!')

    def install(self):
        """Install application files in current location."""
        import pydin as pd

        self._header()
        print('Welcome!')
        print(f'This is an installation of PyDin v.{pd.__version__}.')
        while True:
            sure = input('Continue? [Y/n] ')
            if sure in ('Y', 'n'):
                break
        if sure == 'Y':
            self.create_config()
            self.create_scheduler()
            self._configure_database()
            self._expand_configs()

    def create_scheduler(self):
        """Create scheduler in current location."""
        print('Please follow the steps to create the scheduler.')
        name = input('Enter the name or leave empty:\n')
        desc = input('Enter the description or leave empty:\n')
        self.driver.create_scheduler(name=name, desc=desc)
        print('Scheduler created in current location.')

    def start_scheduler(self):
        """Start scheduler."""
        pid = self.driver.start_scheduler()
        if pid is not None:
            print(f'Scheduler started on PID {pid}.')
        else:
            print('Scheduler failed to start')

    def stop_scheduler(self):
        """Stop scheduler."""
        self.driver.stop_scheduler()
        print('Scheduler stopped.')

    def restart_scheduler(self):
        """Restart scheduler."""
        self.stop_scheduler()
        self.start_scheduler()
        print('Scheduler restarted.')

    def report_scheduler(self):
        """Show current scheduler status."""
        pid = self.driver.report_scheduler()
        if pid is not None:
            print(f'Scheduler is running on PID {pid}.')
        else:
            print('Scheduler is not running.')

    def create_job(self, scenario=None):
        """Create job in scheduler of current location."""
        if scenario is None:
            print('Please follow the steps to create the job.')
            configuration = self._configure_job()
        sure = self._are_you_sure()
        if sure:
            print('Creating job...')
            if scenario is None:
                id = self.driver.create_job(**configuration)
            elif scenario == 'default':
                id = self.driver.create_job()
            print(f'Job created with ID {id}.')
        else:
            print('Operation canceled.')

    def configure_job(self, id):
        """Configure job data in schedule table."""
        id = self._check_id(id)
        print('Please follow the steps to configure the job.')
        configuration = self._configure_job()
        sure = self._are_you_sure()
        if sure:
            print('Editing job...')
            self.driver.configure_job(id, **configuration)
            print('Job edited.')
        else:
            print('Operation canceled.')

    def edit_script(self, id):
        """Open job script in text editor."""
        id = self._check_id(id)
        editor = config['GENERAL'].get('editor')
        path = os.path.join(self.root, f'jobs/{id}/script.py')
        if os.path.exists(path) is True:
            # Launch edition process and wait until it is completed.
            proc = sp.Popen([editor, path])
            print(f'Editing {path}...')
            proc.wait()
        else:
            print(f'File {path} does not exists.')

    def enable_job(self, id):
        """Activate job."""
        id = self._check_id(id)
        result = self.driver.enable_job(id)
        if result is True:
            print(f'Job {id} enabled.')
        else:
            print(f'Job {id} does not exist.')

    def disable_job(self, id):
        """Deactivate job."""
        id = self._check_id(id)
        result = self.driver.disable_job(id)
        if result is True:
            print(f'Job {id} disabled.')
        else:
            print(f'Job {id} does not exist.')

    def delete_job(self, id):
        """Delete job."""
        id = self._check_id(id)
        repr = f'Job[{id}]'
        print(f'You are trying to delete {repr}...')
        sure = input('Are you sure? [delete] ')
        sure = True if sure == 'delete' else False
        if sure is True:
            print(f'{repr} delete confirmed.')
            try:
                self.driver.delete_job(id)
            except Exception:
                print(f'{repr} deleted with errors.')
            else:
                print(f'{repr} deleted.')
        else:
            print(f'{repr} delete was not confirmed.')

    def list_jobs(self, id=None):
        """Show all scheduled jobs."""
        id = self._check_id(id) if id is not None else id
        jobs = list(self.driver.list_jobs(id=id))
        row = '|{id:<4}|{name:<30}|{desc:<35}|{status:<6}|'
        head = row.format(id='ID',
                          name='JOB_NAME',
                          desc='JOB_DESCRIPTION',
                          status='STATUS')
        border = f'+{"":->78}+'
        print(border)
        print(head)
        for job in jobs:
            id = job['id']
            name = job['job_name'] or ''
            if len(name) > 30:
                name = f'{name[:27]}...'
            desc = job['job_description'] or ''
            if len(desc) > 35:
                desc = f'{desc[:31]}...'
            status = job['status'] or ''
            print(border)
            print(row.format(id=id, name=name, desc=desc, status=status))
        print(border)

    def run_job(self, id, *args):
        """Run one chosen job."""
        id = self._check_id(id)
        repr = f'Job[{id}]'
        ln = len(args)
        keys = ('tag', 'date', 'process', 'trigger')
        key = args[0] if ln > 1 and args[0] in keys else None
        value = args[1] if ln > 1 and key is not None else None
        kwargs = {}
        if key is None:
            now = dt.datetime.now()
            timestamp = int(now.timestamp())
            print(f'You are trying to start {repr} for current date...')
            kwargs['tag'] = timestamp
        elif key == 'tag':
            tag = int(value)
            print(f'You are trying to start {repr} using timestamp {tag}...')
            kwargs['tag'] = tag
        elif key == 'date':
            date = dt.datetime.fromisoformat(value)
            print(f'You are trying to start {repr} using date {date}...')
            kwargs['date'] = date
        elif key == 'process':
            record_id = int(value)
            print(f'You are trying to start {repr} as Run[{record_id}]...')
            kwargs['record_id'] = record_id
        elif key == 'trigger':
            trigger_id = int(value)
            print(f'You are trying to trig {repr} from Run[{trigger_id}]...')
            kwargs['trigger_id'] = trigger_id
        kwargs['recycle'] = True if 'recycle' in args else None
        kwargs['debug'] = True if 'debug' in args else None
        kwargs['noalarm'] = False if 'noalarm' in args else None
        kwargs['solo'] = True if 'solo' in args else None

        sure = self.sure or self._are_you_sure()
        if sure:
            proc = self.driver.run_job(id, wait=False, **kwargs)
            now = dt.datetime.now()
            date = '%Y-%m-%d %H:%M:%S'
            print(f'{repr} started on PID {proc.pid}, {now:{date}}.')
            if self.wait:
                print(f'Waiting for {repr} to finish...')
                try:
                    proc.wait()
                except KeyboardInterrupt:
                    now = dt.datetime.now()
                    print(f'\n{repr} canceled, {now:{date}}.')
                else:
                    now = dt.datetime.now()
                    if proc.returncode > 0:
                        print(f'{repr} completed with error, {now:{date}}.')
                        if self.error is True:
                            print(proc.stderr.read().decode())
                    else:
                        print(f'{repr} completed, {now:{date}}.')
                        if self.output is True:
                            print(proc.stdout.read().decode())

    def run_jobs(self, path=None):
        """Run many jobs."""
        path = os.path.abspath(path or 'run.list')
        if os.path.exists(path) is True:
            for row in open(path, 'r').readlines():
                row = row.split()
                ln = len(row)
                if ln >= 1:
                    id = row[0]
                    args = row[1:] if ln > 1 else []
                    self.run_job(id, *args)
                    time.sleep(1)
            clean = input(f'Clean file {path}? [Y] ')
            if clean == 'Y':
                open(path, 'w').close()

    def cancel_job(self, id):
        """Cancel one chosen job runs."""
        id = self._check_id(id)
        repr = f'Job[{id}]'
        print(f'You are trying to cancel all {repr} runs...')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.cancel_job(id)
            print(f'All {repr} runs canceled.')

    def cancel_jobs(self):
        """Cancel all currently running jobs."""
        print('You are trying to cancel all currently running jobs...')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.cancel_jobs()
            print('All jobs canceled.')

    def cancel_run(self, id):
        """Cancel run using its ID."""
        id = self._check_id(id)
        repr = f'Run[{id}]'
        print(f'You are trying to cancel {repr}...')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.cancel_run(id)
            print(f'{repr} canceled.')

    def deactivate_run(self, id):
        """Deactivate run using its ID."""
        id = self._check_id(id)
        repr = f'Run[{id}]'
        print(f'You are trying to deactivate {repr}...')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.deactivate_run(id)
            print(f'{repr} deactivated.')

    def create_config(self):
        """Create global configuration file."""
        config_path = self.driver.create_config()
        if config_path is not None:
            print(f'Global config created ({config_path}).')
            return config.load([config_path])
        else:
            print('Global config was not created.')

    def edit_config(self, type=None):
        """Open chosen configuration file in text editor."""
        editor = config['GENERAL'].get('editor')
        if type == 'local' or type is None:
            from .config import local_config as path
        elif type == 'global':
            from .config import user_config as path
        elif type.isdigit() is True:
            id = self._check_id(type)
            path = os.path.join(self.root, f'jobs/{id}/pydin.ini')
        path = os.path.abspath(path)
        if os.path.exists(path) is True:
            # Launch edition process and wait until it is completed.
            proc = sp.Popen([editor, path])
            print(f'Editing {path}...')
            proc.wait()
        else:
            print(f'File {path} does not exists.')

    def create_repo(self):
        """Create job repository and publish it by URL."""
        url = input('Please give remote Git repository URL: ')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.create_repo(url=url)
            print('Job repository successfully created.')

    def sync_repo(self, id=None):
        """Synchronize job repository."""
        print('You are trying to synchronize job repository...')
        sure = self.sure or self._are_you_sure()
        if sure:
            self.driver.pull_repo()
            kwargs = {}
            if id is not None:
                kwargs['id'] = int(id)
            self.driver.push_repo(**kwargs)
            print('Job repository successfully synchronized.')

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-y', '--yes', dest='sure', action='store_true',
                            required=False, help='do not ask to confirm')
        parser.add_argument('-w', '--wait', dest='wait', action='store_true',
                            required=False, help='wait for job to finish')
        parser.add_argument('-o', '--output', dest='output',
                            action='store_true', required=False,
                            help='show process stdout')
        parser.add_argument('-e', '--error', dest='error',
                            action='store_true', required=False,
                            help='show process stderr')
        args, anons = parser.parse_known_args()
        return args

    def _header(self):
        """Print main application header."""
        path = os.path.join(os.path.dirname(__file__), 'samples/head.txt')
        text = open(path, 'r').read()
        print(text)

    def _are_you_sure(self):
        while True:
            sure = input('Are you sure? [Y/n] ')
            if sure in ('Y', 'n'):
                sure = True if sure == 'Y' else False
                break
        return sure

    def _configure_database(self):
        """Configure database connection."""
        from .config import user_config
        print('Configure the listed options to set up the DB schema.')
        print('Application DB schema will be automatically deployed.')
        vendor_name = input('Enter your DB vendor name: ')
        config['DATABASE']['vendor_name'] = vendor_name
        if vendor_name == 'sqlite':
            path = os.path.abspath('pydin.sqlite3')
            if os.path.exists(path):
                print(f'SQLite DB file {path} already exists!')
                return
            else:
                config['DATABASE']['path'] = path
                db.configure()
        elif vendor_name == 'oracle':
            print('Note: you can skip this items '
                  'and set them up later yourself.')
            driver_name = 'cx_oracle'
            config['DATABASE']['driver_name'] = driver_name
            host = input('DB host address: ')
            port = input('DB port number: ')
            sid = input('DB SID: ')
            service_name = input('DB service name: ')
            username = input('User name: ')
            password = getpass.getpass('User password: ')
            for k, v in dict(host=host, port=port, sid=sid,
                             service_name=service_name,
                             username=username, password=password).items():
                if v:
                    config['DATABASE'][k] = v
            if host and port:
                if sid or service_name:
                    if username and password:
                        db.configure()
            else:
                print(f'Please configure the DB connection in {user_config}. '
                      f'Then deploy the DB schema using scripts from GitHub.')
                return
        else:
            print(f'Sorry. This DB vendor {vendor_name} is not supported.')
            return
        config.save()
        print(f'Schema configured. Check configuration file {user_config}.')
        while True:
            sure = input('Continue? [Y/n] ')
            if sure in ('Y', 'n'):
                if sure == 'Y':
                    db.deploy()
                    if vendor_name == 'sqlite':
                        print(f'Schema deployed in SQLite DB file {path}.')
                    elif vendor_name == 'oracle':
                        address = f'{host}/{sid or service_name}'
                        print(f'Schema deployed in Oracle DB {address}.')
                break

    def _configure_job(self):
        name = input('Name this job:\n') or None
        desc = input('Describe shortly this job:\n') or None
        env = input('Enter the environment or leave empty to use default: ')
        env = env or None

        print()
        print('Schedule this job:')
        mday = input('Month day [1-31] ==== ') or None
        hour = input('Hour [0-23] ========= ') or None
        min = input('Minute [0-59] ======= ') or None
        sec = input('Second [0-59] ======= ') or None
        wday = input('Week day [1-7] ====== ') or None
        yday = input('Year day [1-366] ==== ') or None
        trig = input('Parent Job ID(s) [1...n] ') or None
        trig_list = None
        if isinstance(trig, str):
            if trig.isdigit() or trig == '-':
                trig = int(trig) if trig.isdigit() else trig
                trig = db.null if trig == '-' else trig
                trig_list = db.null
            elif ';' in trig:
                trig_list = trig
                trig = db.null
            else:
                trig = to_none(trig)
                trig_list = to_none(trig)

        print()
        start_date = input('Start date [YYYY-MM-DD HH24:MI:SS] ') or None
        start_date = db.null if start_date is None else None
        end_date = input('End date [YYYY-MM-DD HH24:MI:SS] ') or None
        end_date = db.null if end_date is None else None

        print()
        timeout = input('Set timeout [1...n] ')
        timeout = int(timeout) if timeout.isdigit() else None
        timeout = db.null if timeout is None else timeout
        parallelism = input('Set parallelism degree [1...n] ') or None
        if isinstance(parallelism, str):
            if parallelism.isdigit() or parallelism == 'N':
                parallelism = 'N' if parallelism == '1' else parallelism
            else:
                parallelism = to_none(parallelism)

        print()
        rerun_interval = input('Interval between reruns [1...n] ') or None
        if isinstance(rerun_interval, str):
            if rerun_interval.isdigit() or rerun_interval == '-':
                if rerun_interval.isdigit():
                    rerun_interval = int(rerun_interval)
                else:
                    rerun_interval = db.null
            else:
                rerun_interval = to_none(rerun_interval)
        rerun_limit = input('Limit of reruns number [1...n] ') or None
        if isinstance(rerun_limit, str):
            if rerun_limit.isdigit() or rerun_limit == '-':
                if rerun_limit.isdigit() and rerun_limit != '0':
                    rerun_limit = int(rerun_limit)
                else:
                    rerun_limit = db.null
            else:
                rerun_limit = to_none(rerun_limit)
        rerun_days = input('Limit of days for reruns [1...n] ') or None
        if isinstance(rerun_days, str):
            if rerun_days.isdigit() or rerun_days == '-':
                if rerun_days.isdigit():
                    rerun_days = int(rerun_days)
                else:
                    rerun_days = db.null
            else:
                rerun_days = to_none(rerun_days)
        rerun_period = input('Minutes of rerun window [0-59] ') or None
        if isinstance(rerun_period, str):
            if rerun_period == '-':
                rerun_period = db.null

        print()
        sleep_period = input('Hours of sleep window [0-23] ') or None
        if isinstance(sleep_period, str):
            if sleep_period == '-':
                sleep_period = db.null
        wake_up_period = input('Minutes of wake up window [0-59] ') or None
        if isinstance(wake_up_period, str):
            if wake_up_period == '-':
                wake_up_period = db.null

        print()
        alarm = input('Enable alarming for this job [Y] ')
        alarm = True if alarm == 'Y' else None
        email_list = input('List notification email addresses [a,b,...]: ')
        pattern = r'([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)'
        email_list = ','.join(re.findall(pattern, email_list)) or None
        print()

        result = {'name': name,
                  'desc': desc,
                  'env': env,
                  'mday': mday,
                  'hour': hour,
                  'min': min,
                  'sec': sec,
                  'wday': wday,
                  'yday': yday,
                  'trig': trig,
                  'trig_list': trig_list,
                  'start_date': start_date,
                  'end_date': end_date,
                  'timeout': timeout,
                  'parallelism': parallelism,
                  'rerun_interval': rerun_interval,
                  'rerun_limit': rerun_limit,
                  'rerun_days': rerun_days,
                  'rerun_period': rerun_period,
                  'sleep_period': sleep_period,
                  'wake_up_period': wake_up_period,
                  'alarm': alarm,
                  'email_list': email_list}
        return result

    def _expand_configs(self):
        if not os.path.exists(connector.config):
            open(connector.config, 'w')

    def _check_id(self, id):
        if id.isdigit() is True:
            return int(id)
        else:
            raise TypeError('id must be digit')
