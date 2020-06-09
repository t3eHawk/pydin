"""Contains application core elements."""

import argparse
import ctypes
import datetime as dt
import importlib.util as impu
import os
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

from .config import config
from .db import db
from .logger import logger
from .utils import (locate, register, coalesce, to_datetime, to_timestamp,
                    to_process, to_python)


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

        sysinfo = pe.sysinfo()
        self.server = sysinfo.desc.hostname
        self.user = sysinfo.desc.user
        self.pid = sysinfo.desc.pid
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
            h = db.tables.history
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
            table = db.tables.history
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
                table = db.tables.history
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
                    table = db.tables.history
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

        self.server = sysinfo.desc.hostname
        self.user = sysinfo.desc.user
        self.pid = sysinfo.desc.pid

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
                        f'for date <{self.date}>')
        elif isinstance(self.run_id, int) is True:
            logger.debug(f'{self} declared with Run[{self.run_id}]')
            conn = db.connect()
            table = db.tables.history
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
                            f'for date <{self.date}>')
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
                            f'for date <{self.date}>')
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
        spec = impu.spec_from_file_location(name, file)
        module = impu.module_from_spec(spec)
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
        table = db.tables.history
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
        table = db.tables.history
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
        table = db.tables.history
        updated = dt.datetime.now()
        update = (table.update().values(updated=updated, **kwargs).
                  where(table.c.id == self.source_id))
        conn.execute(update)
        logger.debug(f'Source Run[{self.source_id}] history record updated')
        return updated

    def _done(self):
        self.status = 'D'
        if logger.with_error is True:
            self.text_error = self._parse_text_error()
            logger.info(f'{self} completed with error')
        else:
            logger.info(f'{self} completed')
        pass

    def _error(self):
        logger.error()
        self.status = 'E'
        self.text_error = self._parse_text_error()
        logger.info(f'{self} completed with error')
        pass

    def _cancel(self):
        logger.info(f'Canceling {self}...')
        self.status = 'C'
        conn = db.connect()
        table = db.tables.history
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
            return os.path.basename(logger.file.path)

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
