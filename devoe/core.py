"""Contains application core elements."""

import argparse
import atexit
import ctypes
import datetime as dt
import importlib as il
import importlib.util as ilu
import os
import platform
import re
import signal
import subprocess as sp
import sys
import threading as th
import time as tm
import traceback as tb
import queue

import pepperoni as pe
import sqlalchemy as sa

from .config import config, calendar
from .config import Logging
from .logger import logger
from .db import db

from .utils import locate, register
from .utils import coalesce
from .utils import to_datetime, to_timestamp
from .utils import to_process, to_python


class Scheduler():
    """Represents application scheduler."""

    def __init__(self, name=None, desc=None, owner=None):
        self.root = locate()

        self.conn = db.connect()
        self.logger = logger

        self.moment = None
        self.schedule = None
        self.entry = queue.Queue()
        self.queue = queue.Queue()
        self.procs = {}
        self.chargers = []
        self.executors = []

        self.name = name
        self.desc = desc
        self.owner = owner

        self.server = platform.node()
        self.user = os.getlogin()
        self.pid = os.getpid()
        self.start_date = None
        self.stop_date = None
        self.status = None

        logger.configure(format='{isodate}\t{thread}\t{rectype}\t{message}\n',
                         alarming=False)

        argv = [arg for arg in sys.argv if arg.startswith('-') is False]
        if len(argv) > 1:
            arg = argv[1]
            if arg == 'start':
                file = os.path.abspath(sys.argv[0])
                to_python(file, '--start')
            elif arg == 'stop':
                logger.configure(file=False)
                self.stop()
        else:
            args = self._parse_arguments()
            if args.start is True:
                self.start()
            elif args.stop is True:
                logger.configure(file=False)
                self.stop()
        pass

    def __repr__(self):
        """Represent this scheduler with its name."""
        return self.name

    @property
    def running(self):
        """Check whether scheduler running or not."""
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.code == 'SHD')
        result = conn.execute(select).first()
        status = True if result.status == 'Y' else False
        return status

    def start(self):
        """Launch the scheduler."""
        if self.running is False:
            self._start()
            # Iterate scheduler process.
            while True:
                self._process()
        else:
            raise Exception('scheduler already running')
        pass

    def stop(self):
        """Stop the scheduler."""
        self._terminate()
        pass

    def restart(self):
        """Restart the scheduler."""
        self.stop()
        self.start()
        pass

    def _start(self):
        # Perform all necessary preparations before scheduler start.
        logger.head()
        logger.info('Starting scheduler...')
        self.status = True
        self.start_date = dt.datetime.now()
        self._set_signals()
        self._make_threads()
        self._update_component()
        self._read()
        self._synchronize()
        logger.info(f'Scheduler started on PID[{self.pid}]')
        pass

    def _process(self):
        # Perform scheduler phases.
        self._active()
        self._passive()
        pass

    def _shutdown(self, signum=None, frame=None):
        # Stop this running scheduler.
        logger.info('Stopping scheduler...')
        self._stop()
        logger.info(f'Scheduler on PID[{self.pid}] stopped')
        self._exit()
        pass

    def _stop(self):
        # Perform all necessary actions before scheduler stop.
        self.status = False
        self.stop_date = dt.datetime.now()
        self._update_component()
        pass

    def _terminate(self):
        # Kill running scheduler.
        logger.debug('Killing the scheduler...')
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.code == 'SHD')
        result = conn.execute(select).first()
        pid = result.pid
        logger.debug(f'Scheduler should be on PID[{pid}]')
        try:
            if os.name != 'nt':
                os.kill(pid, signal.SIGTERM)
            else:
                kernel = ctypes.windll.kernel32
                logger.debug(f'Scheduler on PID[{pid}] must be terminated')
                kernel.FreeConsole()
                kernel.AttachConsole(pid)
                kernel.SetConsoleCtrlHandler(False, True)
                kernel.GenerateConsoleCtrlEvent(True, False)
        except (OSError, ProcessLookupError):
            logger.debug(f'Scheduler on PID[{pid}] already terminated')
        else:
            logger.debug(f'Scheduler on PID[{pid}] successfully terminated')
        pass

    def _exit(self):
        # Abort this running scheduler execution.
        logger.debug('Execution will be aborted now')
        sys.exit()
        pass

    def _active(self):
        # Run active scheduler phase.
        self._maintain()
        self._walk()
        pass

    def _passive(self):
        # Run passive scheduler phase.
        self._timeshift()
        pass

    def _timeshift(self):
        # Perform all time-relevant stuff.
        delay = tm.time()-self.moment
        wait = 1-delay
        try:
            tm.sleep(wait)
        except ValueError:
            logger.warning('TIME IS BROKEN!')
            self._synchronize()
        else:
            logger.debug(f'moment={self.moment}, {delay=}, {wait=}')
            self._increment()
        pass

    def _synchronize(self):
        # Synchronize the internal time with the real one.
        logger.debug('Time will be synchronized')
        self.moment = int(tm.time())
        logger.debug('Time synchronized')
        pass

    def _increment(self):
        # Move scheduler internal tm.
        self.moment += 1
        pass

    def _maintain(self):
        # Perform maintenance steps.
        # Update schedule if needed.
        interval = config['SCHEDULER'].get('reschedule')
        if int(self.moment) % interval == 0:
            logger.debug('Schedule will be refreshed now due to interval')
            self._read()
            logger.info('Schedule refreshed')
        # Rerun failed jobs.
        interval = config['SCHEDULER'].get('rerun')
        if int(self.moment) % interval == 0:
            logger.debug('Rerun will be requested now due to interval')
            self._rerun()
            logger.info('Rerun requested')
        pass

    def _read(self):
        # Parse the schedule from the table to the memory.
        logger.debug('Refreshing schedule...')
        self.schedule = list(self._sked())
        if self.schedule:
            logger.debug(f'Schedule refreshed {self.schedule}')
        else:
            logger.debug('Schedule is empty')
        pass

    def _sked(self):
        # Read and parse the schedule from the database table.
        logger.debug('Getting schedule...')
        conn = db.connect()
        table = db.tables.schedule
        select = table.select()
        result = conn.execute(select).fetchall()
        for row in result:
            try:
                job = {'id': row.id,
                       'status': True if row.status == 'Y' else False,
                       'mday': row.monthday,
                       'wday': row.weekday,
                       'hour': row.hour,
                       'min': row.minute,
                       'sec': row.second,
                       'trigger': row.trigger_id,
                       'start': to_timestamp(row.start_date),
                       'end': to_timestamp(row.end_date),
                       'env': row.environment,
                       'args': row.arguments,
                       'timeout': row.timeout}
            except Exception:
                logger.warning()
                continue
            else:
                logger.debug(f'Got Job {job}')
                yield job
        logger.debug('Schedule retrieved')
        pass

    def _walk(self):
        # Walk through the jobs and find out which must be executed.
        now = tm.localtime(self.moment)
        for job in self.schedule:
            try:
                if (
                    job['status'] is True
                    and job['trigger'] is None
                    and self._check(job['mday'], now.tm_mday) is True
                    and self._check(job['wday'], now.tm_wday+1) is True
                    and self._check(job['hour'], now.tm_hour) is True
                    and self._check(job['min'], now.tm_min) is True
                    and self._check(job['sec'], now.tm_sec) is True
                    and (job['start'] is None or job['start'] < self.moment)
                    and (job['end'] is None or job['end'] > self.moment)
                ):
                    self.entry.put((job, self.moment))
            except Exception:
                logger.error()
        pass

    def _rerun(self):
        # Define failed runs and send them on re-execution.
        logger.debug('Requesting rerun...')
        try:
            h = db.tables.run_history
            s = db.tables.schedule
            select = (sa.select([h.c.id, h.c.job_id, h.c.job_date,
                                 h.c.added, h.c.rerun_times,
                                 s.c.status, s.c.start_date, s.c.end_date,
                                 s.c.environment, s.c.arguments,
                                 s.c.timeout, s.c.reruns, s.c.days_rerun]).
                      select_from(sa.join(h, s, h.c.job_id == s.c.id)).
                      where(sa.and_(h.c.status.in_(['E', 'T']),
                                    h.c.rerun_id.is_(None),
                                    h.c.rerun_now.is_(None),
                                    h.c.rerun_done.is_(None))))
            conn = db.connect()
            now = dt.datetime.now()
            interval = config['SCHEDULER'].get('rerun')
            date_to = now-dt.timedelta(seconds=interval)
            result = conn.execute(select).fetchall()
            for row in result:
                try:
                    id = row.job_id
                    moment = to_timestamp(row.job_date)
                    repr = f'Job[{id}:{moment}]'
                    status = True if row.status == 'Y' else False
                    start_date = to_timestamp(row.start_date)
                    end_date = to_timestamp(row.end_date)
                    added = to_datetime(row.added)
                    reruns = coalesce(row.reruns, 0)
                    rerun_times = coalesce(row.rerun_times, 0)
                    days_rerun = coalesce(row.days_rerun, 0)
                    date_from = now-dt.timedelta(days=days_rerun)
                    if (
                        status is True
                        and (start_date is None or start_date < self.moment)
                        and (end_date is None or end_date > self.moment)
                        and rerun_times < reruns
                        and added > date_from
                        and added < date_to
                    ):
                        job = {'id': id,
                               'env': row.environment,
                               'args': row.arguments,
                               'timeout': row.timeout,
                               'run': row.id}
                        logger.debug(f'Job will be sent for rerun {job}')
                        logger.info(f'Adding {repr} to the queue for rerun...')
                        self.queue.put((job, moment))
                        logger.info(f'{repr} added to the queue for rerun')
                except Exception:
                    logger.error()
        except Exception:
            logger.error()
        else:
            logger.debug('Rerun requested')
        pass

    def _check(self, unit, now):
        # Check if empty or *.
        if unit is None or re.match(r'^(\*)$', unit) is not None:
            return True
        # Check if unit is lonely digit and equals to now.
        elif re.match(r'^\d+$', unit) is not None:
            unit = int(unit)
            return True if now == unit else False
        # Check if unit is a cycle and integer division with now is true.
        elif re.match(r'^/\d+$', unit) is not None:
            unit = int(re.search(r'\d+', unit).group())
            if unit == 0:
                return False
            return True if now % unit == 0 else False
        # Check if unit is a range and now is in this range.
        elif re.match(r'^\d+-\d+$', unit) is not None:
            unit = [int(i) for i in re.findall(r'\d+', unit)]
            return True if now in range(unit[0], unit[1] + 1) else False
        # Check if unit is a list and now is in this list.
        elif re.match(r'^\d+,\s*\d+.*$', unit):
            unit = [int(i) for i in re.findall(r'\d+', unit)]
            return True if now in unit else False
        # All other cases is not for the now.
        else:
            return False

    def _charger(self):
        # Add jobs from the entry queue to the execution queue.
        while True:
            if self.entry.empty() is False:
                job, moment = self.entry.get()
                logger.debug('Thread is used now')
                logger.debug(f'Job will be sent for execution {job}')
                self._charge(job, moment)
                logger.debug('Thread is released now')
                self.entry.task_done()
            tm.sleep(1)
        pass

    def _charge(self, job, moment):
        # Add job to the execution queue.
        try:
            id = job['id']
            date = dt.datetime.fromtimestamp(moment)
            repr = f'Job[{id}:{moment}]'
            logger.info(f'Adding {repr} to the queue...')
            logger.debug(f'Creating new run history record for {repr}...')
            conn = db.connect()
            table = db.tables.run_history
            insert = (table.insert().
                      values(job_id=id,
                             job_date=date,
                             added=dt.datetime.now(),
                             status='Q',
                             server=self.server,
                             user=self.user))
            result = conn.execute(insert)
            src = job.copy()
            run = result.inserted_primary_key[0]
            job = {'id': src['id'],
                   'env': src['env'],
                   'args': src['args'],
                   'timeout': src['timeout'],
                   'run': run}
            logger.debug(f'Created run history record {run} for {repr}')
            self.queue.put((job, moment))
        except Exception:
            logger.error()
        else:
            logger.info(f'{repr} added to the queue')
        pass

    def _executor(self):
        # Execute jobs registered in the execution queue.
        while True:
            if self.queue.empty() is False:
                job, moment = self.queue.get()
                logger.debug('Thread is used now')
                logger.debug(f'Job will be executed now {job} ')
                self._execute(job, moment)
                self.queue.task_done()
                logger.debug('Thread is released now')
            tm.sleep(1)
        pass

    def _execute(self, job, moment):
        # Execute job.
        try:
            id = job['id']
            env = job['env'] or 'python'
            args = job['args'] or ''
            timeout = job['timeout']
            run = job['run']
            repr = f'Job[{id}:{moment}]'
            logger.info(f'Initiating {repr}...')
            exe = config['ENVIRONMENTS'].get(env)
            file = os.path.join(self.root, f'jobs/{id}/job.py')
            args += f' run -a --run {run}'
            proc = to_process(exe, file, args)
            logger.info(f'{repr} runs on PID {proc.pid}')
            logger.debug(f'Waiting for {repr} to finish...')
            self.procs[proc.pid] = proc
            proc.wait(timeout)
            self.procs.pop(proc.pid)
        except sp.TimeoutExpired:
            logger.warning(f'{repr} timeout exceeded')
            try:
                conn = db.connect()
                table = db.tables.run_history
                update = (table.update().
                          values(status='T').
                          where(table.c.id == run))
                conn.execute(update)
            except Exception:
                logger.error()
            proc.kill()
        except Exception:
            logger.error()
        else:
            if proc.returncode > 0:
                logger.info(F'{repr} completed with error')
                try:
                    conn = db.connect()
                    table = db.tables.run_history
                    update = (table.update().
                              values(status='E',
                                     text_error=proc.stderr.read().decode()).
                              where(table.c.id == run))
                    conn.execute(update)
                except Exception:
                    logger.error()
            else:
                logger.info(f'{repr} completed')
        pass

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', action='store_true',
                            required=False, help='start new scheduler')
        parser.add_argument('--stop', action='store_true',
                            required=False, help='stop running scheduler')
        args, anons = parser.parse_known_args()
        return args

    def _set_signals(self):
        # Configure signal triggers.
        logger.debug('Setting signal triggers...')
        signal.signal(signal.SIGINT, self._shutdown)
        logger.debug(f'SIGINT trigger for PID[{self.pid}] set')
        signal.signal(signal.SIGTERM, self._shutdown)
        logger.debug(f'SIGTERM trigger for PID[{self.pid}] set')
        pass

    def _make_threads(self):
        # Configure all necessary threads.
        logger.debug('Making threads for this scheduler...')

        number = config['SCHEDULER'].get('chargers')
        target = self._charger
        for i in range(number):
            name = f'ChargerThread-{i}'
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.chargers.append(thread)
        logger.debug(f'{number} chargers made {self.chargers}')

        number = config['SCHEDULER'].get('executors')
        target = self._executor
        for i in range(number):
            name = f'ExecutorThread-{i}'
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.executors.append(thread)
        logger.debug(f'{number} executors made {self.executors}')

        pass

    def _update_component(self):
        # Update component information in database table.
        logger.debug('Updating this component information...')
        conn = db.connect()
        table = db.tables.components
        update = table.update().where(table.c.code == 'SHD')
        if self.status is True:
            update = update.values(status='Y',
                                   start_date=self.start_date,
                                   stop_date=None,
                                   server=self.server,
                                   user=self.user,
                                   pid=self.pid)
        elif self.status is False:
            update = update.values(status='N',
                                   stop_date=self.stop_date)
        conn.execute(update)
        logger.debug('Component information updated')
        pass


class Job():
    """Represents single application job."""

    def __init__(self, id=None, name=None, desc=None, date=None, run=None,
                 persons=None, alarm=None, solo=None, debug=None):
        self.root = locate()
        self._registered = register(self)

        self.conn = db.connect()
        self.logger = logger

        self.id = coalesce(id, self._recognize())
        if isinstance(self.id, int) is False or self.id is None:
            raise ValueError(f'id must be int, not {id.__class__.__name__}')

        schedule = self._parse_schedule()
        args = self._parse_arguments()
        sysinfo = pe.sysinfo()

        self.name = name or schedule['name']
        self.desc = desc or schedule['desc']
        self.date = to_datetime(args.date or date)
        self.run_id = args.run or run
        self.trigger_id = args.trigger
        self.source_id = None
        self.seqno = None

        self.added = None
        self.updated = None
        self.start_date = None
        self.end_date = None
        self.status = None
        self.duration = None

        self.server = platform.node()
        self.user = os.getlogin()
        self.pid = os.getpid()

        # Configure dependent objects.
        self.persons = self._parse_persons(persons or schedule['persons'])
        self.alarm = coalesce(args.noalarm, alarm, schedule['alarm'], False)
        self.debug = coalesce(args.debug, debug, schedule['debug'], False)
        logger.configure(app=self.name, desc=self.desc,
                         debug=self.debug, alarming=self.alarm,
                         smtp={'recipients': self.persons})

        self.file_log = self._parse_file_log()
        self.text_error = None
        self.text_log = None

        self.__solo = args.solo
        self.__auto = args.auto

        # Shortcuts for key objects.
        self.sysinfo = sysinfo
        self.creds = pe.credentials()
        self.email = logger.root.email

        act = sysinfo.anons[0] if len(sysinfo.anons) > 0 else None
        if act is not None:
            if act == 'run':
                self.run()
        pass

    def __repr__(self):
        """Represent this job as its ID and timestamp."""
        if self.date is None:
            return f'Job[{self.id}]'
        elif self.date is not None:
            return f'Job[{self.id}:{to_timestamp(self.date)}]'

    def run(self):
        """Run this particular job."""
        self._announce()
        self._prepare()
        self._start()
        try:
            self._run()
        except Exception:
            self._error()
        else:
            self._done()
        self._end()
        self._trigger()
        pass

    def _announce(self):
        logger.head()
        logger.info(f'{self} named <{self.name}> owns PID[{self.pid}]')
        pass

    def _prepare(self):
        logger.debug('Preparing to run this job...')
        self._set_signals()
        logger.debug('Preparation done')
        pass

    def _start(self):
        logger.info(f'{self} starts...')
        self.start_date = dt.datetime.now()
        self.status = 'S'
        if self.run_id is None:
            logger.debug(f'{self} will run as totally new')
            self.date = to_datetime(self.date or dt.datetime.now())
            self.run_id = self._new()
            logger.info(f'{self} started as Run[{self.run_id}] '
                        f'for date[{self.date}]')
        elif isinstance(self.run_id, int) is True:
            logger.debug(f'{self} declared with Run[{self.run_id}]')
            conn = db.connect()
            table = db.tables.run_history
            select = table.select().where(table.c.id == self.run_id)
            result = conn.execute(select).first()
            if result is None:
                raise ValueError(f'Run[{self.run_id}] was not found')
            elif self.id != result.job_id:
                raise ValueError(f'Run[{self.run_id}] '
                                 f'was for Job[{result.job_id}], '
                                 f'not Job[{self.id}]')
            elif result.status == 'R':
                raise ValueError(f'Run[{self.run_id}] is not finished yet')
            elif result.status in ('Q', 'S'):
                logger.debug(f'This run has normal status {result.status} '
                             f'so {self} will just continue with it')
                self.date = to_datetime(result.job_date)
                self.updated = self._update(start_date=self.start_date,
                                            status=self.status,
                                            pid=self.pid,
                                            file_log=self.file_log)
                logger.info(f'{self} started as Run[{self.run_id}] '
                            f'for date[{self.date}]')
            elif result.status in ('D', 'E', 'C', 'T'):
                logger.debug(f'This run already has status {result.status} '
                             f'so {self} will be executed as rerun')
                self.date = to_datetime(result.job_date)
                self.source_id = self.run_id
                self.seqno = (result.rerun_times or 0)+1
                logger.debug(f'This should be {self.seqno}-th rerun of {self}')
                self.run_id = self._new()
                self.updated = self._update_source(rerun_now='Y')
                logger.info(f'{self} is {self.seqno}-th rerun of '
                            f'initial Run[{self.source_id}]')
                logger.info(f'{self} started as Run[{self.run_id}] '
                            f'for date[{self.date}]')
        else:
            raise TypeError('run ID must be int, '
                            f'not {self.run_id.__class__.__name__}')
        logger.debug(f'{self} started')
        pass

    def _run(self):
        logger.info(f'{self} runs...')
        self.status = 'R'
        self.updated = self._update(status=self.status)
        name = 'script'
        file = f'{self.root}/script.py'
        logger.debug(f'{self} script {file=} now will be executed')
        spec = ilu.spec_from_file_location(name, file)
        module = ilu.module_from_spec(spec)
        spec.loader.exec_module(module)
        logger.debug(f'{self} script {file=} executed')
        pass

    def _end(self):
        logger.info(f'{self} ends...')
        self.end_date = dt.datetime.now()
        self.updated = self._update(end_date=self.end_date,
                                    status=self.status,
                                    text_error=self.text_error)
        if self.source_id is not None:
            rerun_now = None
            rerun_done = 'Y' if self.status == 'D' else None
            self.updated = self._update_source(rerun_times=self.seqno,
                                               rerun_now=rerun_now,
                                               rerun_done=rerun_done)
        self.duration = self.end_date-self.start_date
        logger.debug(f'{self} ended')
        logger.info(f'{self} duration {self.duration.seconds} seconds')
        pass

    def _new(self):
        logger.debug(f'New run history record will be created for {self}')
        conn = db.connect()
        table = db.tables.run_history
        insert = (table.insert().
                  values(job_id=self.id,
                         job_date=self.date,
                         added=self.start_date,
                         start_date=self.start_date,
                         status=self.status,
                         server=self.server,
                         pid=self.pid,
                         user=self.user,
                         trigger_id=self.trigger_id,
                         rerun_id=self.source_id,
                         rerun_seqno=self.seqno,
                         file_log=self.file_log))
        id = conn.execute(insert).inserted_primary_key[0]
        logger.debug(f'Created Run[{id}] history record for {self}')
        return id

    def _update(self, **kwargs):
        logger.debug(f'Updating Run[{self.run_id}] history record '
                     f'with following values {kwargs}')
        conn = db.connect()
        table = db.tables.run_history
        updated = dt.datetime.now()
        update = (table.update().values(updated=self.updated, **kwargs).
                  where(table.c.id == self.run_id))
        conn.execute(update)
        logger.debug(f'Run[{self.run_id}] history record updated')
        return updated

    def _update_source(self, **kwargs):
        logger.debug(f'Updating source Run[{self.source_id}] history record '
                     f'with following values {kwargs}')
        conn = db.connect()
        table = db.tables.run_history
        updated = dt.datetime.now()
        update = (table.update().values(updated=updated, **kwargs).
                  where(table.c.id == self.source_id))
        conn.execute(update)
        logger.debug(f'Source Run[{self.source_id}] history record updated')
        return updated

    def _done(self):
        if logger.with_error is True:
            self.status = 'E'
            self.text_error = self._parse_text_error()
            logger.info(f'{self} completed with error')
        else:
            self.status = 'D'
            logger.info(f'{self} completed')
        pass

    def _error(self):
        self.status = 'E'
        self.text_error = self._parse_text_error()
        logger.error()
        logger.info(f'{self} completed with error')
        pass

    def _cancel(self):
        logger.info(f'Canceling {self}...')
        self.status = 'C'
        conn = db.connect()
        table = db.tables.run_history
        update = (table.update().
                  values(updated=self.updated,
                         status=self.status).
                  where(table.c.id == self.run_id))
        conn.execute(update)
        if self.source_id is not None:
            update = (table.update().
                      values(updated=self.updated,
                             rerun_times=self.seqno,
                             rerun_now=None).
                      where(table.c.id == self.source_id))
            conn.execute(update)
        logger.info(f'{self} canceled')
        pass

    def __cancel(self, signum=None, frame=None):
        # Cancel this running job.
        self._cancel()
        sys.exit()
        pass

    def _recognize(self):
        file = os.path.abspath(sys.argv[0])
        dir = os.path.basename(os.path.dirname(file))
        id = int(dir) if dir.isdigit() is True else None
        return id

    def _parse_schedule(self):
        conn = db.connect()
        table = db.tables.schedule
        select = table.select().where(table.c.id == self.id)
        result = conn.execute(select).first()
        schedule = {'name': result.job_name,
                    'desc': result.job_desc,
                    'debug': True if result.debug == 'Y' else False,
                    'alarm': True if result.alarm == 'Y' else False,
                    'persons': result.persons}
        return schedule

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--auto', action='store_true',
                            required=False, help='declare automatic execution')
        parser.add_argument('-d', '--debug', action='store_true', default=None,
                            required=False, help='enable debug mode')
        parser.add_argument('--id', type=int,
                            required=False, help='job unique ID')
        parser.add_argument('--date', type=dt.datetime.fromisoformat,
                            required=False, help='job date in ISO format')
        parser.add_argument('--run', type=int,
                            required=False, help='job run history ID')
        parser.add_argument('--trigger', type=int,
                            required=False, help='trigger history ID')
        parser.add_argument('--solo', action='store_true',
                            required=False, help='do not trigger other jobs')
        parser.add_argument('--noalarm', action='store_false', default=None,
                            required=False, help='disable alarming')
        args, anons = parser.parse_known_args()
        return args

    def _parse_file_log(self):
        if logger.file.status is True:
            return os.path.relpath(logger.file.path)
        pass

    def _parse_persons(self, persons):
        persons = persons or []
        recipients = logger.root.email.recipients or []
        if isinstance(persons, (str, list)) is True:
            if isinstance(persons, str) is True:
                if ',' in persons:
                    persons = persons.replace(' ', '').split(',')
                else:
                    persons = [persons]
        else:
            persons = []
        persons = [*persons, *recipients]
        return persons

    def _parse_text_error(self):
        err_type, err_value, err_tb = logger.all_errors[-1]
        exception = tb.format_exception(err_type, err_value, err_tb)
        text_error = ''.join(exception)
        return text_error

    def _set_signals(self):
        # Configure signal triggers.
        logger.debug('Setting signal triggers...')
        signal.signal(signal.SIGINT, self.__cancel)
        logger.debug(f'SIGINT trigger set for current PID[{self.pid}]')
        signal.signal(signal.SIGTERM, self.__cancel)
        logger.debug(f'SIGTERM trigger set for current PID[{self.pid}]')
        pass

    def _trigger(self):
        if self.status == 'D' and self.__solo is False:
            logger.debug('Trigger will be performed')
            time = tm.time()
            conn = db.connect()
            table = db.tables.schedule
            select = table.select().where(table.c.trigger_id == self.id)
            result = conn.execute(select).fetchall()
            total = len(result)
            if total > 0:
                logger.info('Trigger of dependent jobs...')
                logger.debug(f'Found {total} potential jobs to trigger')
                for row in result:
                    try:
                        id = row.id
                        repr = f'Job[{id}]'
                        status = True if row.status == 'Y' else False
                        start = to_timestamp(row.start_date) or time-1
                        end = to_timestamp(row.end_date) or time+1
                        if status is False or time > end or time < start:
                            logger.debug(f'{repr} will be skipped '
                                         'due to one of the parameters: '
                                         f'{status=}, {start=}, {end=}')
                            continue
                        logger.info(f'{repr} will be triggered')

                        env = row.environment or 'python'
                        args = row.arguments or ''

                        exe = config['ENVIRONMENTS'].get(env)
                        file = os.path.abspath(f'{self.root}/../{id}/job.py')
                        date = self.date.isoformat()
                        trigger = self.run_id
                        args += f' run -a --date {date} --trigger {trigger}'
                        proc = to_process(exe, file, args=args)
                        logger.info(f'{repr} runs on PID {proc.pid}')
                    except Exception:
                        logger.error()
                logger.info('Trigger of dependent jobs done')
            else:
                logger.debug('No dependent jobs to trigger')
        else:
            logger.debug('Trigger is not required '
                         'due to one of the parameters: '
                         f'status={self.status}, solo={self.__solo}')
        pass


class Task():
    """Represents standalone job task."""

    def __init__(self, name=None, date=None, logging=None, json=None):
        self.id = None
        self.name = name
        self.date = date
        self.job = None
        self.job_id = None
        self.run_id = None
        cache = il.import_module('devoe.cache')
        if hasattr(cache, 'Job') and isinstance(cache.Job, Job):
            logger.debug(f'Using {cache.Job} configuration for {self}...')
            self.job = cache.Job
            self.name = cache.Job.name
            self.date = cache.Job.date
            self.job_id = cache.Job.id
            self.run_id = cache.Job.run_id
            logger.info(f'Used {cache.Job} configuration for {self}')
        self.calendar = calendar.Day(self.date)

        self.logging = logging if isinstance(logging, Logging) else Logging()
        self.logger = self.logging.task.setup()
        logger.debug(f'{self.logger.name} is used for {self} logging')

        self.updated = None
        self.start_date = None
        self.end_date = None
        self._status = None
        self._records_read = 0
        self._records_written = 0
        self._records_found = 0
        self._files_found = 0
        self._bytes_found = 0
        self.errors = []

        self.initiator = 'S' if getattr(self.job, 'auto', 0) > 0 else 'U'
        self.server = platform.node()
        self.user = os.getlogin()
        self.file_log = logger.file.path if logger.file.status else None

        pass

    def __repr__(self):
        """Represent task."""
        if self.id is not None:
            return f'Task[{self.id}]'
        elif self.job_id is not None and self.run_id is not None:
            return f'Task[{self.job.id}:{self.job.run_id}]'
        else:
            return 'Task'
        pass

    @property
    def name(self):
        """Get task name."""
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            self._name = value
        else:
            self._name = None
        pass

    @property
    def date(self):
        """Get task date."""
        return self._date

    @date.setter
    def date(self, value):
        if isinstance(value, dt.datetime):
            self._date = value
        else:
            self._date = dt.datetime.now()
        pass

    @property
    def status(self):
        """Get current task status."""
        return self._status

    @status.setter
    def status(self, value):
        """Change task status."""
        if isinstance(value, str):
            self._status = value
            self.logger.table(status=self.status)
            logger.debug(f'{self} status changed to {value}')
        else:
            message = (f'status must be str not {value.__class__.__name__}')
            raise TypeError(message)
        pass

    @property
    def records_read(self):
        """Get number of records read."""
        return self._records_read

    @records_read.setter
    def records_read(self, value):
        if isinstance(value, int):
            self._records_read += value
            self.logger.table(records_read=self.records_read)
        pass

    @property
    def records_written(self):
        """Get number of records written."""
        return self._records_written

    @records_written.setter
    def records_written(self, value):
        if isinstance(value, int):
            self._records_written += value
            self.logger.table(records_written=self.records_written)
        pass

    @property
    def result_value(self):
        """Get short numeric result value."""
        return self._result_value

    @result_value.setter
    def result_value(self, value):
        if isinstance(value, int):
            self._result_value = value
            self.logger.table(result_value=self.result_value)
        pass

    @property
    def result_long(self):
        """Get long string result value."""
        return self._result_long

    @result_long.setter
    def result_long(self, value):
        if isinstance(value, (list, tuple, dict, str)):
            self._result_long = value
            self.logger.table(result_long=str(self.result_long))
        pass

    @property
    def text_error(self):
        """Get textual exception from the last registered error."""
        if self.errors:
            return ''.join(tb.format_exception(*self.errors[-1]))
        else:
            return None
        pass

    def run(self):
        """Run task."""
        logger.line('-------------------')
        self._prepare()
        self._start()
        self._execute()
        self._end()
        logger.line('-------------------')
        return self.result()

    def prepare(self):
        """Make all necessary task preparations."""
        pass

    def start(self):
        """Start task."""
        pass

    def execute(self):
        """Execute task."""
        pass

    def end(self):
        """End task."""
        pass

    def result(self):
        """Return task result."""
        pass

    def _prepare(self):
        logger.debug(f'Preparing {self}...')
        self.prepare()
        logger.debug(f'{self} prepared')
        pass

    def _start(self):
        logger.info(f'Starting {self}...')
        self.start_date = dt.datetime.now()
        self.status = 'S'
        self.id = self.logger.root.table.primary_key
        if self.id is not None:
            table = self.logger.root.table.proxy
            select = f'SELECT * FROM {table} WHERE id = {self.id}'
            logger.info(f'{self} DB query: {select}')
        self.logger.table(start_date=self.start_date,
                          job_id=self.job_id,
                          run_id=self.run_id,
                          task_date=self.date,
                          **{key: value for key, value in self.__dict__.items()
                             if key in self.logging.task.optional
                             and self.logging.task.fields[key] is True})
        self.start()
        logger.debug(f'{self} started')
        return atexit.register(self._exit)

    def _execute(self):
        logger.info(f'Executing {self}...')
        logger.info(f'{self} steps: {[s.graph for s in self.steps.values()]}')
        try:
            self.status = 'R'
            self.execute()
        except Exception:
            self._error()
        else:
            self._done()
        pass

    def _end(self):
        logger.debug(f'Ending {self}...')
        self.end()
        if self.status == 'D':
            self.end_date = dt.datetime.now()
            self.logger.table(end_date=self.end_date)
        elif self.status == 'E':
            if self.logging.task.fields.get('text_error'):
                self.logger.table(text_error=self.text_error)
        logger.info(f'{self} ended')
        pass

    def _done(self):
        self.status = 'D' if not self.errors else 'E'
        if self.status == 'D':
            logger.info(f'{self} executed')
        elif self.status == 'E':
            logger.info(f'{self} executed with error')
        pass

    def _error(self):
        self.status = 'E'
        self.errors.append((sys.exc_info()))
        logger.info(f'{self} executed with error')
        logger.error()
        pass

    def _exit(self):
        logger.debug(f'{self} exits...')
        if self.status is not None:
            if self.status not in ('D', 'E'):
                logger.debug(f'Caught unexpected end in {self}')
                self.status = 'U'
        logger.debug(f'{self} exit performed')
        pass

    pass


class Pipeline(Task):
    """Represents ETL pipeline."""

    def __init__(self, *items, name=None, date=None, logging=None, json=None):
        super().__init__(name=name, date=date, logging=logging, json=json)
        self.items = {}
        self.steps = {}
        self.threads = []
        self.add(*items)
        pass

    @property
    def roots(self):
        """List all root steps."""
        return [step for step in self.walk() if not step.a.prev]

    def add(self, *items, refresh=True):
        """Add new pipeline items."""
        logger.debug(f'Adding following items to {self}: {items}')
        root = list(self.items)[0] if self.items else None
        for i, item in enumerate(items):
            logger.debug(f'Now {item} will be added to {self}...')
            if not isinstance(item, (Item, list, tuple)):
                message = (f'item must be Item, list or tupple'
                           f'not {item.__class__.__name__}')
                raise TypeError(message)
            elif isinstance(item, (list, tuple)):
                self.add(*item, refresh=False)
                root = None
            else:
                item.attach(self, root=root)
                root = item
        logger.debug(f'Items {items} added to {self}')
        return self.refresh() if refresh is True else None

    def refresh(self):
        """Refresh pipeline steps."""
        logger.debug(f'Generating steps for {self}...')
        items = [value for value in self.items.values()]
        for key in [key for key in self.steps]:
            self.steps.pop(key)
        for i, a in enumerate(items):
            if not isinstance(a, (list, tuple)) and a.extractable:
                for b in a.next:
                    if b.transformable:
                        for c in b.next:
                            if c.loadable:
                                step = Step(a=a, b=b, c=c)
                                step.attach(self)
                                a.join(step)
                    elif b.loadable:
                        step = Step(a=a, b=b)
                        step.attach(self)
                        a.join(step)
            elif not isinstance(a, (list, tuple)) and a.executable:
                step = Step(a=a)
                step.attach(self)
                if i > 0:
                    items[i-1].join(step)
        logger.debug(f'Steps for {self} genererated')
        return self.steps

    def walk(self):
        """Walk through the pipeline steps one by one."""
        for step in self.steps.values():
            yield step
        pass

    def execute(self):
        """Run all pipeline steps."""
        for step in self.roots:
            logger.debug(f'Found {step} in roots of {self}')
            self.to_thread(step)
        return self.wait()

    def to_thread(self, step):
        """Create new thread for step execution."""
        logger.debug(f'Creating thread for {step} in {self}...')
        name = f'StepThread-{step.seqno}'
        target = step.run
        step.thread = th.Thread(name=name, target=target, daemon=True)
        logger.debug(f'Created {step.thread.name} for {step} in {self}')
        self.threads.append(step.thread)
        logger.debug(f'Starting {step.thread.name}...')
        return step.thread.start()

    def wait(self):
        """Wait until all threads are finished."""
        for thread in self.threads:
            logger.debug(f'Waiting for {thread.name} of {self} to finish...')
            thread.join()
            logger.debug(f'{thread.name} of {self} finished')
        pass

    pass


class Element():
    """Represents pipeline elements such as Item or Step."""

    def __repr__(self):
        """Represent element with its name."""
        return self.name

    @property
    def pipeline(self):
        """Get element pipeline."""
        return self._pipeline

    @pipeline.setter
    def pipeline(self, value):
        """Set element pipeline."""
        if isinstance(value, Pipeline) or value is None:
            self._pipeline = value
        else:
            message = (f'pipeline must be Pipeline, '
                       f'not {value.__class__.__name__}')
            raise TypeError(message)
        pass

    @property
    def name(self):
        """Get element name."""
        return self._name

    @name.setter
    def name(self, value):
        """Set element name."""
        if isinstance(value, str):
            self._name = value
        else:
            message = f'name must be str, not {value.__class__.__name__}'
            raise TypeError(message)
        pass

    @property
    def seqno(self):
        """Get element sequence number."""
        return self._seqno

    @seqno.setter
    def seqno(self, value):
        """Set element sequence number."""
        if isinstance(value, int) or value is None:
            self._seqno = value
        else:
            message = f'seqno must be int, not {value.__class__.__name__}'
            raise TypeError(message)
        pass

    def rename(self, new_name):
        """Change element name."""
        self.name = new_name
        pass

    def sort(self, new_seqno):
        """Change element sequence number."""
        self.seqno = new_seqno
        pass

    def attach(self, pipeline):
        """Assign element to the pipeline passed as argument."""
        self.pipeline = pipeline
        pass

    pass


class Item(Element):
    """Represents single step item."""

    def __init__(self, name=None, seqno=None, pipeline=None):
        self.name = name or __class__.__name__
        self.seqno = seqno
        self.thread = None
        self.pipeline = pipeline
        self._prev = []
        self._next = []
        self._steps = []
        pass

    @property
    def item_name(self):
        """Get item name."""
        return self.name

    @item_name.setter
    def item_name(self, value):
        """Set item name."""
        self.name = value
        pass

    @property
    def prev(self):
        """List all connected items that precede this one."""
        return self._prev

    @prev.setter
    def prev(self, item):
        """Connect the item as a preceding to this one."""
        if isinstance(item, Item) and item not in self._prev:
            self._prev.append(item)
            item.next = self
        pass

    @property
    def next(self):
        """List all connected items that proceed this one."""
        return self._next

    @next.setter
    def next(self, item):
        """Connect the item as a proceeding to this one."""
        if isinstance(item, Item) and item not in self._next:
            self._next.append(item)
        pass

    @property
    def joins(self):
        """List all steps where the item is a join."""
        return self._steps

    def attach(self, pipeline, root=None):
        """Assign item to the pipeline passed as argument."""
        self.pipeline = pipeline
        self.prev = root
        name = self.name
        seqno = 1
        while True:
            if self.pipeline.items.get(name) is None:
                self.sort(seqno)
                if seqno > 1:
                    self.rename(name)
                break
            elif self.pipeline.items.get(name) == self:
                return
            else:
                seqno += 1
                name = f'{self.name}-{seqno}'
        self.pipeline.items[name] = self
        logger.debug(f'{self} attached to {pipeline}')
        pass

    def join(self, step):
        """Make item a join between current step and following steps."""
        if isinstance(step, Step):
            if step not in self._steps:
                self._steps.append(step)
        else:
            message = (f'step must be Step, not {step.__class__.__name__}')
            raise TypeError(message)
        pass

    def prepare(self):
        """Prepare Item."""
        pass

    pass


class Step(Element):
    """Represents single task step."""

    def __init__(self, name=None, seqno=None, a=None, b=None, c=None,
                 pipeline=None):
        self.id = None
        self.name = name or __class__.__name__
        self.seqno = seqno
        self.thread = None
        self.threads = []
        self.queue = queue.Queue()
        self.input = queue.Queue()
        self.output = queue.Queue()

        assert a is None or isinstance(a, Item)
        self.a = a
        assert b is None or isinstance(b, Item)
        self.b = b
        assert c is None or isinstance(c, Item)
        self.c = c
        if pipeline is not None:
            self.attach(pipeline)

        self.start_date = None
        self.end_date = None
        self._status = None
        self._records_read = 0
        self._records_written = 0
        self._records_found = 0
        self._result_value = 0
        self._result_long = 0
        self.errors = []

        self.initiator = None
        self.server = platform.node()
        self.user = os.getlogin()
        self.file_log = logger.file.path if logger.file.status else None

        pass

    def __repr__(self):
        """Represent step."""
        if self.id is not None:
            return f'{self.name}[{self.id}]'
        else:
            return self.name
        pass

    @property
    def step_name(self):
        """Get item name."""
        return self.name

    @step_name.setter
    def step_name(self, value):
        """Set item name."""
        self.rename(value)
        pass

    @property
    def type(self):
        """Determine step type."""
        if self.a.executable:
            return 'X'
        elif self.a.extractable and self.b.loadable:
            return 'EL'
        elif self.a.extractable and self.b.transformable and self.c.loadable:
            return 'ETL'
        else:
            return None
        pass

    @property
    def graph(self):
        """Get string representing step structure."""
        if self.a is not None and self.b is None and self.c is None:
            return f'-->{self.a}-->'
        elif self.a is not None and self.b is not None and self.c is None:
            return f'{self.a}-->{self.b}'
        elif self.a is not None and self.b is not None and self.c is not None:
            return f'{self.a}-->{self.b}-->{self.c}'
        else:
            return None
        pass

    @property
    def first(self):
        """Get first item in the step."""
        return self.a

    @property
    def last(self):
        """Get last item in the step."""
        if self.b is None and self.c is None:
            return self.a
        elif self.c is None:
            return self.b
        else:
            return self.c
        pass

    @property
    def status(self):
        """Get current step status."""
        return self._status

    @status.setter
    def status(self, value):
        """Change step status."""
        if isinstance(value, str):
            self._status = value
            self.logger.table(status=self.status)
            logger.debug(f'{self} status changed to {value}')
        else:
            message = (f'status must be str not {value.__class__.__name__}')
            raise TypeError(message)
        pass

    @property
    def records_read(self):
        """Get number of records read."""
        return self._records_read

    @records_read.setter
    def records_read(self, value):
        if isinstance(value, int):
            self._records_read += value
            self.pipeline.records_read = value
            self.logger.table(records_read=self.records_read)
        pass

    @property
    def records_written(self):
        """Get number of records written."""
        return self._records_written

    @records_written.setter
    def records_written(self, value):
        if isinstance(value, int):
            self._records_written += value
            self.pipeline.records_written = value
            self.logger.table(records_written=self.records_written)
        pass

    @property
    def result_value(self):
        """Get short numeric result value."""
        return self._result_value

    @result_value.setter
    def result_value(self, value):
        if isinstance(value, int):
            self._result_value = value
            self.pipeline.result_value = value
            self.logger.table(result_value=self.result_value)
        pass

    @property
    def result_long(self):
        """Get long string result value."""
        return self._result_long

    @result_long.setter
    def result_long(self, value):
        if isinstance(value, (list, tuple, dict, str)):
            self._result_long = value
            self.pipeline.result_long = value
            self.logger.table(result_long=str(self.result_long))
        pass

    @property
    def extraction(self):
        """Get extraction state."""
        if self.type in ['ETL', 'EL'] and self.a.thread.is_alive() is True:
            return True
        else:
            return False
        pass

    @property
    def transformation(self):
        """Get transformation state."""
        if self.type == 'ETL' and self.b.thread.is_alive() is True:
            return True
        else:
            return False
        pass

    @property
    def loading(self):
        """Get loading state."""
        if self.type == 'ETL' and self.c.thread.is_alive() is True:
            return True
        if self.type == 'EL' and self.b.thread.is_alive() is True:
            return True
        else:
            return False
        pass

    @property
    def text_error(self):
        """Get textual exception from the last registered error."""
        if self.errors:
            return ''.join(tb.format_exception(*self.errors[-1]))
        else:
            return None
        pass

    def attach(self, pipeline):
        """Attach step to the pipeline passed as argument."""
        self.pipeline = pipeline
        name = Step.__name__
        seqno = 1
        while True:
            if self.pipeline.steps.get(name) is None:
                self.sort(seqno)
                if seqno > 1:
                    self.rename(name)
                break
            elif (
                self.pipeline.steps.get(name).a == self.a
                and self.pipeline.steps.get(name).b == self.b
                and self.pipeline.steps.get(name).c == self.c
            ):
                return
            else:
                seqno += 1
                name = f'{self.name}-{seqno}'
        self.pipeline.steps[name] = self
        self.date = self.pipeline.date
        self.initiator = self.pipeline.initiator
        self.logging = self.pipeline.logging
        self.logger = self.pipeline.logging.step.setup(self)
        logger.debug(f'{self.logger.name} is used for {self} logging')
        logger.debug(f'{self}: {self.graph} - attached to {pipeline}')
        pass

    def run(self):
        """Run step."""
        self._start()
        self._execute()
        self._end()
        return self.resume()

    def resume(self):
        """Proceed execution by running all joined steps."""
        if self.status == 'D':
            for step in self.last.joins:
                self.pipeline.to_thread(step)
        pass

    def _start(self):
        logger.info(f'Starting {self}...')
        self.start_date = dt.datetime.now()
        self.status = 'S'
        self.id = self.logger.root.table.primary_key
        if self.id is not None:
            table = self.logger.root.table.proxy
            select = f'SELECT * FROM {table} WHERE id = {self.id}'
            logger.info(f'{self} DB query: {select}')
        self.logger.table(start_date=self.start_date,
                          task_id=self.pipeline.id,
                          job_id=self.pipeline.job_id,
                          run_id=self.pipeline.run_id,
                          step_name=self.name,
                          step_type=self.type,
                          step_date=self.date,
                          **{key: value for key, value in self.__dict__.items()
                             if key in self.logging.step.optional
                             and self.logging.step.fields[key] is True})
        if self.a is not None:
            self.logger.table(step_a=self.a.item_name)
            if self.b is not None:
                self.logger.table(step_b=self.b.item_name)
                if self.c is not None:
                    self.logger.table(step_c=self.c.item_name)
        logger.debug(f'{self} started')
        return atexit.register(self._exit)

    def _execute(self):
        logger.info(f'Executing {self}...')
        logger.info(f'{self} is {self.type} operation: {self.graph}')
        try:
            self.status = 'R'
            if self.type == 'ETL':
                self.c.prepare()
                self.a.to_extractor(self, self.input)
                self.b.to_transformer(self, self.input, self.output)
                self.c.to_loader(self, self.output)
            elif self.type == 'EL':
                self.b.prepare()
                self.a.to_extractor(self, self.queue)
                self.b.to_loader(self, self.queue)
            elif self.type == 'X':
                self.to_executor(self)
            for thread in self.threads:
                logger.debug(f'Waiting for {thread.name} to finish...')
                thread.join()
                logger.debug(f'{thread.name} finished')
        except Exception:
            self._error()
        else:
            self._done()
        pass

    def _end(self):
        logger.debug(f'Ending {self}...')
        if self.status == 'D':
            self.end_date = dt.datetime.now()
            self.logger.table(end_date=self.end_date)
        elif self.status == 'E':
            if self.logging.step.fields.get('text_error'):
                self.logger.table(text_error=self.text_error)
        logger.info(f'{self} ended')
        pass

    def _done(self):
        self.status = 'D' if not self.errors else 'E'
        if self.status == 'D':
            logger.info(f'{self} executed')
        elif self.status == 'E':
            logger.info(f'{self} executed with error')
        pass

    def _error(self):
        self.status = 'E'
        self.errors.append(sys.exc_info())
        self.pipeline.errors.append(self.errors[-1])
        logger.info(f'{self} executed with error')
        logger.error()
        pass

    def _exit(self):
        logger.debug(f'{self} exits...')
        if self.status is not None:
            if self.status not in ('D', 'E'):
                logger.debug(f'Caught unexpected end in {self}')
                self.status = 'U'
        logger.debug(f'{self} exit performed')
        pass

    pass
