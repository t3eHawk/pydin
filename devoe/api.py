"""Contains Python API including prototypes and built-in models."""

import configparser
import ctypes
import git
import os
import shutil
import signal
import sys
import time as tm

import sqlalchemy as sa

from .config import config
from .logger import logger
from .db import db

from .utils import to_process, to_python
from .utils import locate
from .wrap import check_repo


class Driver():
    """Represents application Python API."""

    def __init__(self, root=None):
        self.root = locate() if root is None else os.path.abspath(root)
        self.jobs = os.path.join(self.root, 'jobs')

        logger.configure(file=False, alarming=False)
        pass

    def create_scheduler(self, name=None, desc=None, path=None):
        """Deploy scheduler with all necessary elements."""
        logger.debug(f'Creating scheduler...')
        root = self.root if path is None else os.path.abspath(path)
        samples = os.path.join(os.path.dirname(__file__), 'samples')
        dir = self.jobs
        if os.path.exists(dir) is False:
            os.makedirs(dir)
            logger.debug(f'Folder {dir} created')
        else:
            raise Exception(f'folder {dir} already exists!')
        filename = 'scheduler'
        src = os.path.join(samples, f'{filename}.txt')
        dest = os.path.join(root, f'{filename}.py')
        if os.path.exists(dest) is False:
            content = open(src, 'r').read()
            with open(dest, 'w') as fh:
                fh.write(content)
                logger.debug(f'File {dest} created')
        else:
            raise Exception(f'file {dest} already exists!')
        config_path = os.path.join(root, 'devoe.ini')
        config_local = configparser.ConfigParser()
        config_dict = {'SCHEDULER': {'name': name or '',
                                     'desc': desc or '',
                                     'chargers': '5',
                                     'executors': '20',
                                     'reschedule': '60',
                                     'rerun': '3600'},
                       'LOGGING': {'console': 'True',
                                   'file': 'True',
                                   'info': 'True',
                                   'debug': 'False',
                                   'error': 'True',
                                   'warning': 'True',
                                   'critical': 'True',
                                   'alarming': 'False',
                                   'maxsize': '10485760',
                                   'maxdays': '1'}}
        config_local.read_dict(config_dict)
        with open(config_path, 'w') as fh:
            config_local.write(fh, space_around_delimiters=False)
        logger.debug(f'Configuration file {config_path} saved')
        # Not implemented yet.
        # db.deploy()
        # logger.debug(f'Schema deployed at {db}')
        pass

    def start_scheduler(self, path=None):
        """Start scheduler."""
        logger.debug('Starting scheduler...')
        root = self.root if path is None else os.path.abspath(path)
        file = os.path.join(root, 'scheduler.py')
        if os.path.exists(file) is True:
            proc = to_python(file, '--start')
            result = proc.poll()
            running = True if result is None else False
            if result is None:
                timer = 0.25
                while timer > 0:
                    wait = 0.01
                    tm.sleep(wait)
                    timer -= 0.01
                    if result != proc.poll():
                        running = False
                        break
            if running is True:
                logger.debug('Scheduler started')
                return proc.pid
            else:
                logger.debug('Scheduler failed to start')
                return None
        else:
            logger.debug('Scheduler cannot be started')
            raise Exception(f'file {file} does not exist')
        pass

    def stop_scheduler(self, path=None):
        """Stop scheduler."""
        logger.debug('Stopping scheduler...')
        root = self.root if path is None else os.path.abspath(path)
        file = os.path.join(root, 'scheduler.py')
        if os.path.exists(file) is True:
            to_python(file, 'stop')
            logger.debug('Scheduler stopped')
        else:
            logger.debug('Scheduler cannot be stopped')
            raise Exception(f'file {file} does not exist')
        pass

    def restart_scheduler(self, path=None):
        """Restart scheduler."""
        self.stop_scheduler(path=path)
        self.start_scheduler(path=path)
        pass

    def report_scheduler(self):
        """Define current scheduler status."""
        logger.debug('Checking scheduler current status...')
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'SCHEDULER')
        result = conn.execute(select).first()
        status = True if result.status == 'Y' else False
        if status is False:
            logger.debug('Scheduler is not running according to status')
            return None
        else:
            pid = result.pid
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                logger.debug('Scheduler should be running but process is dead')
                return None
            else:
                logger.debug('Scheduler is running')
                return result.pid
        pass

    def create_job(self, name=None, desc=None, mday=None, wday=None,
                   hour=None, min=None, sec=None, tgid=None,
                   start_date=None, end_date=None, env=None, args=None,
                   timeout=None, maxreruns=None, maxdays=None,
                   alarm=None, recipients=None, debug=None, norepo=False):
        """Create job with all necessary elements."""
        logger.debug('Creating job...')
        conn = db.connect()
        table = db.tables.schedule
        values = db.normalize(job=name, description=desc, status=False,
                              monthday=mday, weekday=wday,
                              hour=hour, minute=min, second=sec,
                              trigger_id=tgid,
                              start_date=start_date,
                              end_date=end_date,
                              environment=env, arguments=args,
                              timeout=timeout,
                              maxreruns=maxreruns,
                              maxdays=maxdays,
                              alarm=alarm,
                              recipients=recipients,
                              debug=debug)
        logger.debug(f'Configuring schedule record with {values=}')
        insert = table.insert().values(**values)
        id = conn.execute(insert).inserted_primary_key[0]
        repr = f'Job[{id}]'
        logger.debug(f'{repr} schedule record configured')

        root = self.root
        samples = os.path.join(os.path.dirname(__file__), 'samples')
        folder = os.path.join(root, f'jobs/{id}')

        if os.path.exists(folder) is False:
            os.makedirs(folder)
            logger.debug(f'Folder {folder} created')
        else:
            raise Exception(f'folder {folder} already exists!')

        filenames = ['job', 'script']
        for filename in filenames:
            src = os.path.join(samples, f'{filename}.txt')
            dest = os.path.join(folder, f'{filename}.py')
            if os.path.exists(dest) is False:
                content = open(src, 'r').read()
                with open(dest, 'w') as fh:
                    fh.write(content)
                    logger.debug(f'File {dest} created')
            else:
                raise Exception(f'file {dest} already exists!')

        config_path = os.path.join(folder, 'devoe.ini')
        config_local = configparser.ConfigParser()
        config_dict = {'JOB': {},
                       'LOGGING': {'console': 'True',
                                   'file': 'True',
                                   'info': 'True',
                                   'debug': 'False',
                                   'error': 'True',
                                   'warning': 'True',
                                   'critical': 'True',
                                   'alarming': 'True',
                                   'maxsize': '10485760',
                                   'maxdays': '1'},
                       'EMAIL': {'toggle': 'True'}}
        config_local.read_dict(config_dict)
        with open(config_path, 'w') as fh:
            config_local.write(fh, space_around_delimiters=False)
        logger.debug(f'Configuration file {config_path} saved')

        path = os.path.join(root, 'jobs/.git')
        if norepo is False and os.path.exists(path) is True:
            self.push_repo(id=id)

        logger.debug(f'{repr} successfully created and scheduled')
        return id

    def configure_job(self, id, name=None, desc=None, mday=None, wday=None,
                      hour=None, min=None, sec=None, tgid=None,
                      start_date=None, end_date=None, env=None, args=None,
                      timeout=None, maxreruns=None, maxdays=None,
                      alarm=None, recipients=None, debug=None):
        """Modify job configuration."""
        repr = f'Job[{id}]'
        logger.debug(f'Editing {repr}...')
        conn = db.connect()
        table = db.tables.schedule
        values = db.normalize(job=name, description=desc,
                              monthday=mday, weekday=wday,
                              hour=hour, minute=min, second=sec,
                              trigger_id=tgid,
                              start_date=start_date,
                              end_date=end_date,
                              environment=env, arguments=args,
                              timeout=timeout,
                              maxreruns=maxreruns,
                              maxdays=maxdays,
                              alarm=alarm,
                              recipients=recipients,
                              debug=debug)
        if len(values) > 0:
            logger.debug(f'Configuring schedule record with {values=}')
            update = table.update().values(**values).where(table.c.id == id)
            conn.execute(update)
            logger.debug(f'{repr} edited')
        else:
            logger.debug(f'{repr} nothing to edit')
        pass

    def enable_job(self, id):
        """Change job status to Y."""
        repr = f'Job[{id}]'
        logger.debug(f'{repr} will be enabled')
        conn = db.connect()
        table = db.tables.schedule
        update = table.update().values(status='Y').where(table.c.id == id)
        result = conn.execute(update)
        if result.rowcount > 0:
            logger.debug(f'{repr} enabled')
            return True
        else:
            logger.debug(f'{repr} does not exist')
            return False
        pass

    def disable_job(self, id):
        """Change job status to N."""
        repr = f'Job[{id}]'
        logger.debug(f'{repr} will be disabled')
        conn = db.connect()
        table = db.tables.schedule
        update = table.update().values(status='N').where(table.c.id == id)
        result = conn.execute(update)
        if result.rowcount > 0:
            logger.debug(f'{repr} disabled')
            return True
        else:
            logger.debug(f'{repr} does not exist')
            return False
        pass

    def delete_job(self, id):
        """Delete particular job."""
        repr = f'Job[{id}]'
        logger.debug(f'Requested to delete {repr}')
        root = self.root
        conn = db.connect()
        table = db.tables.schedule
        delete = table.delete().where(table.c.id == id)
        conn.execute(delete)
        logger.debug(f'{repr} deleted from schedule')
        folder = os.path.join(root, f'jobs/{id}')
        shutil.rmtree(folder)
        path = os.path.join(root, 'jobs/.git')
        if os.path.exists(path) is True:
            self.push_repo(id=id)
        logger.debug(f'{repr} folder {folder} removed')
        pass

    def list_jobs(self, id=None):
        """Generate list with all scheduled jobs."""
        logger.debug('Requested to list jobs')
        conn = db.connect()
        table = db.tables.schedule
        select = table.select()
        if id is not None:
            select = select.where(table.c.id == id)
        result = conn.execute(select).fetchall()
        for row in result:
            yield dict(row)
        logger.debug('Jobs listed')
        pass

    def run_job(self, id, tag=None, date=None, record_id=None, trigger_id=None,
                wait=True, debug=None, noalarm=None, solo=None):
        """Run particular job."""
        repr = f'Job[{id}]'
        logger.debug(f'Requested to run {repr}')
        conn = db.connect()
        table = db.tables.schedule
        select = table.select().where(table.c.id == id)
        result = conn.execute(select).first()
        env = result.environment or 'python'
        args = result.arguments or ''
        exe = config['ENVIRONMENTS'].get(env)
        file = os.path.join(self.root, f'jobs/{id}/job.py')
        args += ' run'
        args_dict = {'tag': tag,
                     'date': date.isoformat() if date is not None else None,
                     'record': record_id,
                     'trigger': trigger_id,
                     'debug': '' if debug is True else None,
                     'noalarm': '' if noalarm is False else None,
                     'solo': '' if solo is True else None}
        for key, value in args_dict.items():
            if value is not None:
                args += f' --{key} {value}'
        logger.debug(f'{exe=}, {file=}, {args=}')
        proc = to_process(exe, file, args=args)
        logger.debug(f'{repr} runs on PID {proc.pid}')
        if wait is True:
            logger.debug(f'Waiting for {repr} to finish...')
            proc.wait()
            if proc.returncode > 0:
                logger.debug(f'{repr} completed with error')
            else:
                logger.debug(f'{repr} completed')
        return proc

    def cancel_job(self, id):
        """Cancel particular job."""
        repr = f'Job[{id}]'
        logger.debug(f'Requested to cancel {repr} runs...')
        conn = db.connect()
        table = db.tables.history
        select = (table.select().
                  where(sa.and_(table.c.job_id == id,
                                table.c.status == 'R')))
        result = conn.execute(select).fetchall()
        for row in result:
            run = row.id
            logger.debug(f'Found {repr} running as Run[{run}]')
            self.cancel_run(run)
        logger.debug(f'All {repr} runs canceled')
        pass

    def cancel_jobs(self):
        """Cancel all jobs."""
        logger.debug(f'Requested to cancel all jobs...')
        conn = db.connect()
        table = db.tables.history
        select = table.select().where(table.c.status == 'R')
        result = conn.execute(select).fetchall()
        for row in result:
            job = row.job_id
            run = row.id
            logger.debug(f'Found Job[{job}] running as Run[{run}]')
            self.cancel_run(run)
        logger.debug(f'All jobs canceled')
        pass

    def cancel_run(self, id):
        """Cancel particular run."""
        repr = f'Run[{id}]'
        logger.debug(f'Requested to cancel {repr}...')
        conn = db.connect()
        table = db.tables.history
        select = table.select().where(table.c.id == id)
        result = conn.execute(select).first()
        if result.status == 'R':
            pid = result.pid
            try:
                if os.name != 'nt':
                    os.kill(pid, signal.SIGTERM)
                else:
                    kernel = ctypes.windll.kernel32
                    logger.debug(f'{repr} on PID[{pid}] must be terminated')
                    kernel.FreeConsole()
                    kernel.AttachConsole(pid)
                    kernel.SetConsoleCtrlHandler(False, True)
                    kernel.GenerateConsoleCtrlEvent(True, False)
            except (OSError, ProcessLookupError, TypeError):
                logger.debug(f'{repr} on PID[{pid}] already terminated')
                update = (table.update().
                          values(status='C').
                          where(sa.and_(table.c.id == id,
                                        table.c.status == 'R')))
                conn.execute(update)
                logger.debug(f'{repr} status changed to C')
            else:
                logger.debug(f'{repr} on PID[{pid}] successfully terminated')
            logger.debug(f'{repr} canceled')
        else:
            raise Exception(f'{repr} is not running')
        pass

    def create_config(self):
        """Create global config."""
        logger.debug('Creating global config...')
        from .config import user as path
        config_path = os.path.abspath(path)
        if os.path.exists(config_path) is True:
            raise Exception(f'file {config_path} already exists!')
        else:
            config_parser = configparser.ConfigParser()
            config_dict = {'GENERAL': {'debug': '',
                                       'editor': '',
                                       'owner': ''},
                           'DATABASE': {'vendor': '',
                                        'driver': '',
                                        'path': '',
                                        'host': '',
                                        'port': '',
                                        'sid': '',
                                        'service': '',
                                        'user': '',
                                        'password': ''},
                           'EMAIL': {'toggle': '',
                                     'host': '',
                                     'port': '',
                                     'tls': '',
                                     'address': '',
                                     'user': '',
                                     'password': ''},
                           'API': {'host': '',
                                   'port': '',
                                   'token': ''},
                           'ENVIRONMENTS': {'python': sys.executable}}
            config_parser.read_dict(config_dict)
            with open(config_path, 'w') as fh:
                config_parser.write(fh, space_around_delimiters=False)
            logger.debug(f'Global config {config_path} created')
            return config_path

    def create_repo(self, url=None):
        """Create git repository."""
        logger.debug('Creating git repo...')
        try:
            repo = git.Repo(self.jobs)
        except git.exc.InvalidGitRepositoryError:
            repo = git.Repo.init(self.jobs)
            logger.debug(f'Git repo {repo.common_dir} initiated')
            repo.create_remote('origin', url)
            logger.debug('Remote <origin> created')
            commit = repo.index.commit('Initial commit')
            logger.debug(f'Initial commit made as <{commit.hexsha}>')
            origin = repo.remote('origin')
            origin.push(repo.head.ref.name, set_upstream=True)
            logger.debug(f'Git repo {repo.common_dir} published in {url}')

            filename = 'gitignore'
            samples = os.path.join(os.path.dirname(__file__), 'samples')
            src = os.path.join(samples, f'{filename}.txt')
            dest = os.path.join(self.jobs, f'.gitignore')
            if os.path.exists(dest) is False:
                content = open(src, 'r').read()
                with open(dest, 'w') as fh:
                    fh.write(content)
                    logger.debug(f'File {dest} created')
        else:
            raise Exception(f'git repo in {repo.common_dir} already exists!')
        return repo

    @check_repo
    def push_repo(self, id=None, message=None):
        """Commit all current changes and push git repo to the remote."""
        logger.debug('Pushing git repo...')
        timestamp = int(tm.time())
        repo = git.Repo(self.jobs)
        if id is None:
            repo.git.add(all=True)
            logger.debug('All files staged')
        else:
            repr = f'Job[{id}]'
            logger.debug(f'Requested to push only {repr}')
            folder = os.path.join(self.jobs, str(id))
            untracked_files = repo.untracked_files
            unstaged_files = [diff.a_path for diff in repo.index.diff(None)]
            all_files = [*untracked_files, *unstaged_files]
            for file in all_files:
                abspath = os.path.join(self.jobs, file)
                commonpath = os.path.commonpath([folder, abspath])
                if folder == commonpath:
                    logger.debug(f'Found {repr} file {file} for stage')
                    repo.index.add(file)

        staged_files = repo.index.diff(repo.head.name)
        count = len(staged_files)
        logger.debug(f'{count} files staged')
        if count > 0:
            message = message or f'devoe[{timestamp}]'
            commit = repo.index.commit(message)
            logger.debug(f'This {message} commit made as <{commit.hexsha}>')
            origin = repo.remote('origin')
            result = origin.push(repo.head.ref.name)
            logger.debug(f'Git repo {repo.common_dir} successfully pushed')
            return result
        else:
            logger.debug(f'Nothing to push in {repo.common_dir}')
        pass

    @check_repo
    def pull_repo(self):
        """Pull git repo from the remote to get all external changes."""
        logger.debug('Pulling git repo...')
        repo = git.Repo(self.jobs)
        origin = repo.remote('origin')
        result = origin.pull()
        logger.debug(f'Git repo {repo.common_dir} successfully pulled')
        return result
