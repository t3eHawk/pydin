"""Contains application core elements."""

import argparse
import atexit
import ctypes
import datetime as dt
import importlib as imp
import importlib.util as impu
import json
import os
import pickle
import platform
import re
import signal
import subprocess as sp
import sys
import threading as th
import time as tm
import traceback as tb
import types
import queue

import pepperoni as pe
import sqlalchemy as sa

from .db import db
from .config import config
from .config import connector
from .config import calendar
from .logger import logger

from .utils import locate
from .utils import cache, declare
from .utils import coalesce
from .utils import to_datetime, to_timestamp
from .utils import to_process, to_python

from .config import Logging
from .config import Localhost, Server, Database


class Scheduler():
    """Represents application scheduler."""

    def __init__(self, name=None, desc=None, owner=None):
        self.path = locate()

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
        select = table.select().where(table.c.id == 'SCHEDULER')
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
        select = table.select().where(table.c.id == 'SCHEDULER')
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
                       'tgid': row.trigger_id,
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
                    and job['tgid'] is None
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
            select = (sa.select([h.c.id, h.c.job_id, h.c.run_tag,
                                 h.c.added, h.c.rerun_times,
                                 s.c.status, s.c.start_date, s.c.end_date,
                                 s.c.environment, s.c.arguments,
                                 s.c.timeout, s.c.maxreruns, s.c.maxdays]).
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
                    tag = row.run_tag
                    repr = f'Job[{id}:{tag}]'
                    status = True if row.status == 'Y' else False
                    start_date = to_timestamp(row.start_date)
                    end_date = to_timestamp(row.end_date)
                    added = to_datetime(row.added)
                    maxreruns = coalesce(row.maxreruns, 0)
                    maxdays = coalesce(row.maxdays, 0)
                    rerun_times = coalesce(row.rerun_times, 0)
                    date_from = now-dt.timedelta(days=maxdays)
                    if (
                        status is True
                        and (start_date is None or start_date < self.tag)
                        and (end_date is None or end_date > self.tag)
                        and rerun_times < maxreruns
                        and added > date_from
                        and added < date_to
                    ):
                        job = {'id': id,
                               'env': row.environment,
                               'args': row.arguments,
                               'timeout': row.timeout,
                               'record_id': row.id}
                        logger.debug(f'Job will be sent for rerun {job}')
                        logger.info(f'Adding {repr} to the queue for rerun...')
                        self.queue.put((job, tag))
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
            tag = moment
            date = dt.datetime.fromtimestamp(tag)
            repr = f'Job[{id}:{moment}]'
            logger.info(f'Adding {repr} to the queue...')
            logger.debug(f'Creating new run history record for {repr}...')
            conn = db.connect()
            table = db.tables.run_history
            insert = (table.insert().
                      values(job_id=id,
                             run_tag=tag,
                             run_date=date,
                             added=dt.datetime.now(),
                             status='Q',
                             server=self.server,
                             user=self.user))
            result = conn.execute(insert)
            src = job.copy()
            record_id = result.inserted_primary_key[0]
            job = {'id': src['id'],
                   'env': src['env'],
                   'args': src['args'],
                   'timeout': src['timeout'],
                   'record_id': record_id}
            logger.debug(f'Created run history record {record_id} for {repr}')
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
            record_id = job['record_id']
            repr = f'Job[{id}:{moment}]'
            logger.info(f'Initiating {repr}...')
            exe = config['ENVIRONMENTS'].get(env)
            file = os.path.join(self.path, f'jobs/{id}/job.py')
            args += f' run -a --record {record_id}'
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
                          where(table.c.id == record_id))
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
                              where(table.c.id == record_id))
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
            name = f'Charger-{i}'
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.chargers.append(thread)
        logger.debug(f'{number} chargers made {self.chargers}')

        number = config['SCHEDULER'].get('executors')
        target = self._executor
        for i in range(number):
            name = f'Executor-{i}'
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
        update = table.update().where(table.c.id == 'SCHEDULER')
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

    def __init__(self, id=None, name=None, desc=None, tag=None, date=None,
                 record_id=None, trigger_id=None, recipients=None,
                 alarm=None, debug=None, solo=None):
        self.conn = db.connect()
        self.path = locate()
        self.cached = cache(self)
        self.declared = declare(self)

        self.id = id
        schedule = self._parse_schedule()
        args = self._parse_arguments()
        self.args = args

        self.name = name or schedule['name']
        self.desc = desc or schedule['desc']
        self.mode = 'A' if args.auto is True else 'M'

        history = db.tables.run_history
        self.record_id = args.record or record_id
        self.record = db.record(history, self.record_id)
        if self.record_id:
            result = self.record.read()
            if result and self.id == result.job_id:
                self.tag = result.run_tag
                self.added = result.added
                self.updated = result.updated
                self.start_date = result.start_date
                self.end_date = result.end_date
                self.status = result.status
                self.reruns = result.rerun_times
                self.seqno = result.rerun_seqno

                if result.data_dump:
                    self.data = pickle.loads(result.data_dump)
            else:
                run = f'run[{self.record_id}]'
                message = f'no such {self} having {run}'
                raise ValueError(message)
        else:
            self.tag = coalesce(args.tag, args.date, tag, date, tm.time())
            self.added = None
            self.updated = None
            self.start_date = None
            self.end_date = None
            self.status = None
            self.reruns = None
            self.seqno = None

            self.data = types.SimpleNamespace()
            self.errors = set()

        self.trigger_id = args.trigger or trigger_id
        self.trigger = db.record(history, self.trigger_id)

        self.initiator_id = None
        self.initiator = db.record(history, self.initiator_id)

        # Configure dependent objects.
        recipients = coalesce(recipients, schedule['recipients'])
        self.recipients = self._parse_recipients(recipients)

        self.auto = args.auto
        self.solo = args.solo
        self.alarm = coalesce(args.mute, alarm, schedule['alarm'], False)
        self.debug = coalesce(args.debug, debug, schedule['debug'], False)
        logger.configure(app=self.name, desc=self.desc,
                         debug=self.debug, alarming=self.alarm,
                         smtp={'recipients': self.recipients})

        self.server = platform.node()
        self.user = os.getlogin()
        self.pid = os.getpid()
        self.file_log = self._parse_file_log()
        self.text_log = None

        # Shortcuts for key objects.
        self.logger = logger
        self.sysinfo = pe.sysinfo()
        self.creds = pe.credentials()
        self.email = logger.email

        act = self.sysinfo.anons[0] if len(self.sysinfo.anons) > 0 else None
        if act is not None:
            if act == 'run':
                self.run()
        pass

    def __repr__(self):
        """Represent this job as its ID and timestamp."""
        if self.tag is None:
            return f'Job[{self.id}]'
        elif self.tag is not None:
            if self.record_id is None:
                return f'Job[{self.id}:{self.tag}]'
            elif self.record_id is not None:
                return f'Job[{self.id}:{self.tag}:{self.record_id}]'
        pass

    @property
    def id(self):
        """."""
        if hasattr(self, '_id'):
            return self._id
        pass

    @id.setter
    def id(self, value):
        if self.id is None:
            value = coalesce(value, self._recognize())
            if isinstance(value, int):
                self._id = value
            else:
                class_name = value.__class__.__name__
                message = f'id must be int, not {class_name}'
                raise ValueError(message)
        else:
            message = 'id cannot be redefined'
            raise AttributeError(message)
        pass

    @property
    def tag(self):
        """."""
        if hasattr(self, '_tag'):
            return self._tag
        pass

    @tag.setter
    def tag(self, value):
        if isinstance(value, (int, float, str, dt.datetime)):
            self._tag, self._date = to_timestamp(value), to_datetime(value)
        elif value is None:
            self._tag, self._date = None, None
        else:
            requires = 'int, float, str or datetime'
            class_name = value.__class__.__name__
            message = f'tag must be {requires}, not {class_name}'
            raise ValueError(message)
        pass

    @property
    def date(self):
        """."""
        if hasattr(self, '_date'):
            return self._date
        pass

    @date.setter
    def date(self, value):
        if isinstance(value, (str, dt.datetime)) or value is None:
            self.tag = value
        else:
            requires = 'str or datetime'
            class_name = value.__class__.__name__
            message = f'date must be {requires}, not {class_name}'
            raise ValueError(message)
        pass

    @property
    def record_id(self):
        """."""
        return self._record_id

    @record_id.setter
    def record_id(self, value):
        if value is None or isinstance(value, int):
            self._record_id = value
        else:
            message = f'run ID must be int, not {value.__class__.__name__}'
            raise TypeError(message)
        pass

    @property
    def duration(self):
        """."""
        return self.end_date-self.start_date

    @property
    def text_parameters(self):
        """."""
        if self.data:
            return json.dumps(self.data, sort_keys=True)
        else:
            return None
        pass

    @property
    def text_error(self):
        """Get textual exception from the last registered error."""
        if self.errors:
            err_type, err_value, err_tb = list(self.errors)[-1]
            return ''.join(tb.format_exception(err_type, err_value, err_tb))
        else:
            return None
        pass

    @property
    def pipeline(self):
        """Build pipeline for this job if configured."""
        if self.configured:
            nodes = []
            models = imp.import_module('devoe.models')
            variables = imp.import_module('devoe.vars')
            for record in self.configuration:
                source_name = record['source_name']
                node_name = record['node_name']
                node_type = record['node_type']
                node_config = json.loads(record['node_config'])
                custom_query = record['custom_query']

                date_field = record['date_field']
                days_back = record['days_back']
                hours_back = record['hours_back']
                months_back = record['months_back']
                timezone = record['timezone']
                value_field = record['value_field']

                key_name = record['key_field']
                key_field = getattr(variables, key_name) if key_name else None

                chunk_size = record['chunk_size']
                cleanup = True if record['cleanup'] else False

                constructor = getattr(models, node_type)
                node = constructor(model_name=node_name,
                                   source_name=source_name,
                                   custom_query=custom_query,
                                   date_field=date_field,
                                   days_back=days_back,
                                   hours_back=hours_back,
                                   months_back=months_back,
                                   timezone=timezone,
                                   value_field=value_field,
                                   key_field=key_field,
                                   chunk_size=chunk_size,
                                   cleanup=cleanup,
                                   **node_config)
                nodes.append(node)
            return Pipeline(*nodes)

    @property
    def configured(self):
        """Check if job configured."""
        return True if self.configuration else False

    @property
    def configuration(self):
        """Get job configuration."""
        conn = db.connect()
        jc = db.tables.job_config
        select = jc.select().where(jc.c.job_id == self.id).\
                             order_by(jc.c.node_seqno)
        result = conn.execute(select)
        if result:
            return [dict(record) for record in result]
        else:
            return None

    @classmethod
    def get(cls):
        """Get existing job from current execution."""
        cache = imp.import_module('devoe.cache')
        object = getattr(cache, cls.__name__.lower())
        return object

    @classmethod
    def exists(cls):
        """Check if job exists in current execution."""
        cache = imp.import_module('devoe.cache')
        if hasattr(cache, cls.__name__.lower()):
            return True
        else:
            return False
        pass

    def run(self):
        """Run this particular job."""
        self._announce()
        self._prepare()
        self._start()
        try:
            self._run()
        except Exception:
            self._fail()
        self._end()
        self._trig()
        pass

    def store(self, **kwargs):
        """Store the given arguments in a special job namespace."""
        new = {k: v for k, v in kwargs.items() if k not in self.data.__dict__}
        return self.data.__dict__.update(new)

    def _announce(self):
        logger.head()
        name = f'name[{self.name}]'
        pid = f'PID[{self.pid}]'
        logger.info(f'{self} having {name} launched on {pid}')
        pass

    def _prepare(self):
        logger.debug('Preparing to run this job...')
        self._set_signals()
        logger.debug('Preparation done')
        pass

    def _start(self):
        logger.info(f'{self} starts...')
        if self.record_id is None:
            self.start_date = dt.datetime.now()
            self.status = 'S'
            self._start_as_new()
        else:
            logger.debug(f'{self} declared as run[{self.record_id}]')
            if self.status == 'Q':
                self.start_date = dt.datetime.now()
                self.status = 'S'
                self._start_as_continue()
            elif self.status == 'S':
                message = f'{self} run[{self.record_id}] already starting'
                raise ValueError(message)
            elif self.status == 'R':
                message = f'{self} run[{self.record_id}] already running'
                raise ValueError(message)
            elif self.status in ('D', 'E', 'C', 'T'):
                self.added = dt.datetime.now()
                self.start_date = dt.datetime.now()
                self.end_date = None
                self.status = 'S'
                self.seqno = (self.reruns or 0)+1
                self.reruns = None
                self.initiator_id = self.initiator.select(self.record_id)
                self._start_as_rerun()

        run = f'run[{self.record_id}]'
        tag = f'tag[{self.tag}]'
        logger.info(f'{self} started as {run} using {tag}')
        logger.info(f'{self} reports for date - {self.date}')

        logger.debug(f'{self} started')
        pass

    def _start_as_new(self):
        logger.debug(f'{self} will be executed as totally new')

        self.record_id = self.record.create()
        self.record.write(job_id=self.id,
                          run_mode=self.mode,
                          run_tag=self.tag,
                          run_date=self.date,
                          added=self.start_date,
                          start_date=self.start_date,
                          status=self.status,
                          trigger_id=self.trigger_id,
                          rerun_id=self.initiator_id,
                          rerun_seqno=self.seqno,
                          server=self.server,
                          user=self.user,
                          pid=self.pid,
                          file_log=self.file_log)
        pass

    def _start_as_continue(self):
        logger.debug(f'{self} will be executed as continue')

        self.record.write(run_mode=self.mode,
                          start_date=self.start_date,
                          status=self.status,
                          server=self.server,
                          user=self.user,
                          pid=self.pid,
                          file_log=self.file_log)
        pass

    def _start_as_rerun(self):
        logger.debug(f'{self} will be executed as rerun')

        run = f'run[{self.initiator_id}]'
        logger.info(f'{self} {self.seqno}-th rerun initiated from {run}')

        self.record_id = self.record.create()
        self.record.write(job_id=self.id,
                          run_mode=self.mode,
                          run_tag=self.tag,
                          run_date=self.date,
                          added=self.start_date,
                          start_date=self.start_date,
                          status=self.status,
                          trigger_id=self.trigger_id,
                          rerun_id=self.initiator_id,
                          rerun_seqno=self.seqno,
                          server=self.server,
                          user=self.user,
                          pid=self.pid,
                          file_log=self.file_log)
        self.initiator.write(rerun_now='Y')
        pass

    def _run(self):
        logger.info(f'{self} runs...')
        self.status = 'R'
        self.record.write(status=self.status)
        if self.configured:
            logger.debug(f'{self} pipeline now will be performed')
            self.pipeline.run()
            logger.debug(f'{self} pipeline performed')
        else:
        name = 'script'
            path = f'{self.path}/script.py'
            logger.debug(f'{self} script {path=} now will be executed')
            spec = impu.spec_from_file_location(name, path)
        module = impu.module_from_spec(spec)
        spec.loader.exec_module(module)
            logger.debug(f'{self} script {path=} executed')
        pass

    def _fail(self):
        logger.error()
        return self.__fail()

    def __fail(self):
        self.errors.add(sys.exc_info())
        pass

    def _end(self):
        logger.debug(f'{self} ends...')
        self.status = self._done() if not self.errors else self._error()
        self.end_date = dt.datetime.now()
        self.record.write(end_date=self.end_date, status=self.status,
                          text_error=self.text_error,
                          data_dump=pickle.dumps(self.data))
        if self.initiator_id:
            rerun_done = 'Y' if self.status == 'D' else None
            self.initiator.write(rerun_times=self.seqno,
                                 rerun_now=None, rerun_done=rerun_done)
        logger.debug(f'{self} ended')
        logger.info(f'{self} duration {self.duration.seconds} seconds')
        pass

    def _done(self):
        logger.info(f'{self} completed')
        return 'D'

    def _error(self):
        logger.info(f'{self} completed with error')
        return 'E'

    def _cancel(self):
        logger.info(f'Canceling {self}...')
        self.status = 'C'
        self.record.write(status=self.status)
        if self.initiator_id:
            self.initiator.write(rerun_times=self.seqno, rerun_now=None)
        logger.info(f'{self} canceled')
        pass

    def __cancel(self, signum=None, frame=None):
        # Cancel this running job.
        self._cancel()
        return sys.exit()

    def _recognize(self):
        file = os.path.abspath(sys.argv[0])
        dir = os.path.basename(os.path.dirname(file))
        id = int(dir) if dir.isdigit() is True else None
        return id

    def _parse_schedule(self):
        table = db.tables.schedule
        record = db.record(table, self.id)
        result = record.read()
        schedule = {'name': result.job,
                    'desc': result.description,
                    'debug': True if result.debug == 'Y' else False,
                    'alarm': True if result.alarm == 'Y' else False,
                    'recipients': result.recipients}
        return schedule

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--auto', action='store_true',
                            required=False, help='declare automatic execution')
        parser.add_argument('-d', '--debug', action='store_true', default=None,
                            required=False, help='enable debug mode')
        parser.add_argument('--id', type=int,
                            required=False, help='job unique ID')
        parser.add_argument('--tag', type=int,
                            required=False, help='job run timestamp')
        parser.add_argument('--date', type=dt.datetime.fromisoformat,
                            required=False, help='job run date in ISO format')
        parser.add_argument('--record', type=int,
                            required=False, help='job run ID')
        parser.add_argument('--trigger', type=int,
                            required=False, help='trigger ID')
        parser.add_argument('--solo', action='store_true',
                            required=False, help='do not trigger other jobs')
        parser.add_argument('--mute', action='store_false', default=None,
                            required=False, help='disable alarming')
        args, anons = parser.parse_known_args()
        return args

    def _parse_file_log(self):
        if logger.file.status is True:
            return os.path.relpath(logger.file.path)
        pass

    def _parse_recipients(self, recipients):
        recipients = recipients or []
        if isinstance(recipients, (str, list)) is True:
            if isinstance(recipients, str) is True:
                if ',' in recipients:
                    recipients = recipients.replace(' ', '').split(',')
                else:
                    recipients = [recipients]
        else:
            recipients = []
        owners = logger.root.email.recipients or []
        recipients = [*recipients, *owners]
        return recipients

    def _set_signals(self):
        # Configure signal triggers.
        logger.debug('Setting signal triggers...')
        signal.signal(signal.SIGINT, self.__cancel)
        logger.debug(f'SIGINT trigger set for current PID[{self.pid}]')
        signal.signal(signal.SIGTERM, self.__cancel)
        logger.debug(f'SIGTERM trigger set for current PID[{self.pid}]')
        pass

    def _trig(self):
        if self.status == 'D' and self.solo is False:
            logger.debug(f'{self} requesting other jobs to trigger...')
            time = tm.time()
            conn = db.connect()
            table = db.tables.schedule
            select = table.select().where(table.c.trigger_id == self.id)
            result = conn.execute(select).fetchall()
            total = len(result)
            if total > 0:
                logger.debug(f'{self} found {total} potential jobs to trigger')
                logger.info(f'{self} triggering other jobs...')
                for row in result:
                    try:
                        id = row.id
                        tag = self.tag
                        trigger_id = self.record_id
                        repr = f'Job[{id}:{tag}]'
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
                        file = os.path.abspath(f'{self.path}/../{id}/job.py')
                        args += f' run -a --tag {tag} --trigger {trigger_id}'
                        proc = to_process(exe, file, args=args)
                        logger.info(f'{repr} runs on PID {proc.pid}')
                    except Exception:
                        logger.error()
                logger.info(f'{self} other jobs triggered')
            else:
                logger.debug(f'{self} no other jobs triggered')
        else:
            logger.debug('Trigger is not required '
                         'due to one of the parameters: '
                         f'status={self.status}, solo={self.solo}')
        pass


class Pipeline():
    """Represents ETL pipeline."""

    def __init__(self, *nodes, name=None, date=None, logging=None):
        self.name = name or __class__.__name__
        self.date = date

        self.task = Task()
        self.steps = {}
        self.nodes = {}
        self.threads = []

        self.job = Job.get() if Job.exists() else None
        if self.job:
            logger.debug(f'Using {self.job} configuration in {self}...')
            if self.job.name:
                self.name = self.job.name
            if self.job.date:
                self.date = self.job.date
        self.logging = logging if isinstance(logging, Logging) else Logging()
        self.calendar = calendar.Day(self.date)

        self.task.setup(self)
        self.add(*nodes)
        pass

    def __repr__(self):
        """Represent pipeline with its name."""
        return self.name

    @property
    def name(self):
        """Get pipeline name."""
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
        """Get report date."""
        return self._date

    @date.setter
    def date(self, value):
        if isinstance(value, dt.datetime):
            self._date = value
        else:
            self._date = dt.datetime.now()
        pass

    @property
    def order(self):
        """Get list of nodes in order of steps."""
        return list(self.nodes.values())

    @property
    def roots(self):
        """List all root steps."""
        return [step for step in self.walk() if not step.a.prev]

    def add(self, *nodes):
        """Add new pipeline nodes."""
        self.bind(*nodes)
        return self.refresh()

    def bind(self, *nodes):
        """Extend pipeline using nodes listed."""
        root = self.order[0] if self.nodes else None
        for node in nodes:
            if not isinstance(node, (Node, list, tuple)):
                requires = 'Node, list or tupple'
                class_name = node.__class__.__name__
                message = (f'argument must be {requires} not {class_name}')
                raise TypeError(message)
            elif isinstance(node, (list, tuple)):
                self.bind(*node)
                root = None
            else:
                node.setup(self, root)
                root = node
        return self.nodes

    def refresh(self):
        """Refresh pipeline structure step by step."""
        self.steps.clear()
        nodes = [value for value in self.nodes.values()]
        for i, a in enumerate(nodes):
            if not isinstance(a, (list, tuple)) and a.extractable:
                for b in a.next:
                    if b.transformable:
                        for c in b.next:
                            if c.loadable:
                                step = Step(a=a, b=b, c=c)
                                step.setup(self)
                                a.join(step)
                    elif b.loadable:
                        step = Step(a=a, b=b)
                        step.setup(self)
                        a.join(step)
            elif not isinstance(a, (list, tuple)) and a.executable:
                step = Step(a=a)
                step.setup(self)
                if i > 0:
                    nodes[i-1].join(step)
        return self.steps

    def walk(self):
        """Walk through the pipeline plan step by step."""
        for step in self.steps.values():
            yield step
        pass

    def run(self):
        """Run pipeline."""
        return self.task.run()

    pass


class Process():
    """Represents base class for pipeline processes."""

    def __repr__(self):
        """Represent process with its name."""
        return self.name

    @property
    def pipeline(self):
        """Get process pipeline."""
        return self._pipeline

    @pipeline.setter
    def pipeline(self, value):
        if isinstance(value, Pipeline) or value is None:
            self._pipeline = value
        else:
            class_name = value.__class__.__name__
            message = (f'pipeline must be Pipeline, not {class_name}')
            raise TypeError(message)
        pass

    @property
    def job(self):
        """Get process job ID."""
        if self.pipeline and self.pipeline.job:
                return self.pipeline.job

    @property
    def process_name(self):
        """Get process name."""
        return self.name

    @property
    def name(self):
        """Get process name."""
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            self._name = value
        else:
            self._name = None
        pass

    pass


class Unit():
    """Represents base class for pipeline units."""

    def __repr__(self):
        """Represent unit with its name."""
        return self.name

    @property
    def pipeline(self):
        """Get unit task."""
        return self._pipeline

    @pipeline.setter
    def pipeline(self, value):
        if isinstance(value, Pipeline) or value is None:
            self._pipeline = value
        else:
            class_name = value.__class__.__name__
            message = (f'pipeline must be Pipeline, not {class_name}')
            raise TypeError(message)
        pass

    @property
    def job(self):
        """Get process job ID."""
        if self.pipeline and self.pipeline.job:
                return self.pipeline.job

    @property
    def unit_name(self):
        """Get unit name."""
        return self.name

    @property
    def name(self):
        """Get unit name."""
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            self._name = value
        else:
            message = f'name must be str, not {value.__class__.__name__}'
            raise TypeError(message)
        pass

    @property
    def seqno(self):
        """Get unit sequence number."""
        return self._seqno

    @seqno.setter
    def seqno(self, value):
        if isinstance(value, int) or value is None:
            self._seqno = value
        else:
            message = f'seqno must be int, not {value.__class__.__name__}'
            raise TypeError(message)
        pass

    def rename(self, new_name):
        """Change unit name."""
        self.name = new_name
        pass

    def renumber(self, new_seqno):
        """Change unit sequence number."""
        self.seqno = new_seqno
        pass

    pass


class Task(Process):
    """Represents pipeline task."""

    def __init__(self, task_name=None, pipeline=None):
        self.id = None
        self.task_name = task_name or self.__class__.__name__
        if pipeline is not None:
            self.setup(pipeline)

        self.start_date = None
        self.end_date = None
        self._status = None
        self._records_read = 0
        self._records_written = 0
        self._records_found = 0
        self._files_found = 0
        self._bytes_found = 0
        self.errors = set()
        self.updated = None
        pass

    def __repr__(self):
        """Represent task."""
        if self.id is not None:
            return f'Task[{self.id}]'
        else:
            return 'Task'

    @property
    def task_name(self):
        return self.name

    @task_name.setter
    def task_name(self, value):
        self.name = value
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
            err_type, err_value, err_tb = list(self.errors)[-1]
            return ''.join(tb.format_exception(err_type, err_value, err_tb))
        else:
            return None
        pass

    def setup(self, pipeline):
        """Configure the task for the given pipeline."""
        self.pipeline = pipeline
        self.logger = self.pipeline.logging.task.setup()
        logger.debug(f'Logger {self.logger.name} is used in {self} logging')
        logger.debug(f'{self} configured in {pipeline}')
        pass

    def run(self):
        """Run task."""
        self._prepare()
        self._start()
        try:
            self._execute()
        except Exception:
            self._fail()
        self._end()
        return self.result()

    def prepare(self):
        """Make all necessary task preparations."""
        pass

    def start(self):
        """Start task."""
        pass

    def execute(self):
        """Execute pipeline task from the very first pipeline steps."""
        for step in self.pipeline.roots:
            logger.debug(f'Found {step} in roots in {self} for execution')
            step.to_thread()
        return self.wait()

    def wait(self):
        """Wait until all pipeline threads are finished."""
        for thread in self.pipeline.threads:
            logger.debug(f'Waiting for {thread.name} in {self} to finish...')
            thread.join()
            logger.debug(f'{thread.name} in {self} finished')
        pass

    def fail(self):
        """Execute in case of fail."""
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
            logger.debug(f'{self} DB query: {select}')
        self.logger.table(start_date=self.start_date,
                          job_id=self.job.id if self.job else None,
                          run_id=self.job.record_id if self.job else None,
                          task_name=self.name,
                          task_date=self.pipeline.date)
        atexit.register(self._exit)
        logger.debug(f'{self} started')
        return self.start()

    def _execute(self):
        logger.info(f'{self} running')
        self.status = 'R'
        return self.execute()

    def _fail(self):
        logger.error()
        return self.__fail()

    def __fail(self):
        exc_info = sys.exc_info()
        self.errors.add(exc_info)
        if self.job is not None:
            self.job.errors.add(exc_info)
        return self.fail()

    def _end(self):
        logger.debug(f'Ending {self}...')
        self.status = self._done() if not self.errors else self._error()
        if self.status == 'D':
            self.end_date = dt.datetime.now()
            self.logger.table(end_date=self.end_date)
        elif self.status == 'E':
            if self.pipeline.logging.task.fields.get('text_error'):
                self.logger.table(text_error=self.text_error)
        logger.debug(f'{self} ended')
        return self.end()

    def _done(self):
        logger.info(f'{self} done')
        return 'D'

    def _error(self):
        logger.info(f'{self} failed')
        return 'E'

    def _exit(self):
        logger.debug(f'{self} exits...')
        if self.status is not None:
            if self.status not in ('D', 'E'):
                logger.debug(f'Caught unexpected end in {self}')
                self.status = 'U'
        pass

    pass


class Step(Process, Unit):
    """Represents task step."""

    def __init__(self, step_name=None, a=None, b=None, c=None, pipeline=None):
        self.id = None
        self.step_name = step_name or self.__class__.__name__
        self.seqno = None
        self.thread = None
        self.threads = []
        self.queue = queue.Queue()
        self.input = queue.Queue()
        self.output = queue.Queue()

        assert a is None or isinstance(a, Node)
        self.a = a
        assert b is None or isinstance(b, Node)
        self.b = b
        assert c is None or isinstance(c, Node)
        self.c = c
        if pipeline is not None:
            self.setup(pipeline)

        self.start_date = None
        self.end_date = None
        self._status = None
        self._records_read = 0
        self._records_written = 0
        self._records_found = 0
        self._result_value = 0
        self._result_long = 0
        self.errors = set()
        self.updated = None
        pass

    def __repr__(self):
        """Represent step."""
        if self.id is not None:
            return f'{self.name}[{self.id}]'
        else:
            return self.name

    @property
    def step_name(self):
        """Get step name."""
        return self.name

    @step_name.setter
    def step_name(self, value):
        self.name = value
        pass

    @property
    def type(self):
        """Determine step type."""
        if self.a.executable:
            return 'EX'
        elif self.a.extractable and self.b.loadable:
            return 'EL'
        elif self.a.extractable and self.b.transformable and self.c.loadable:
            return 'ETL'
        else:
            return None

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
            self.task.records_read = value
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
            self.task.records_written = value
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
            self.task.result_value = value
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
            self.task.result_long = value
            self.logger.table(result_long=str(self.result_long))
        pass

    @property
    def extraction(self):
        """Get extraction state."""
        if self.type in ['ETL', 'EL'] and self.a.thread.is_alive() is True:
            return True
        else:
            return False

    @property
    def transformation(self):
        """Get transformation state."""
        if self.type == 'ETL' and self.b.thread.is_alive() is True:
            return True
        else:
            return False

    @property
    def loading(self):
        """Get loading state."""
        if self.type == 'ETL' and self.c.thread.is_alive() is True:
            return True
        if self.type == 'EL' and self.b.thread.is_alive() is True:
            return True
        else:
            return False

    @property
    def text_error(self):
        """Get textual exception from the last registered error."""
        if self.errors:
            err_type, err_value, err_tb = list(self.errors)[-1]
            return ''.join(tb.format_exception(err_type, err_value, err_tb))
        else:
            return None

    def setup(self, pipeline):
        """Configure the step for the given pipeline."""
        self.pipeline = pipeline
        self.task = pipeline.task
        name = Step.__name__
        seqno = 1
        while True:
            if self.pipeline.steps.get(name) is None:
                self.renumber(seqno)
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
        self.logger = self.pipeline.logging.step.setup(self)
        logger.debug(f'Logger {self.logger.name} is used in {self} logging')
        logger.debug(f'{self} {self.graph} configured in {pipeline}')
        pass

    def run(self):
        """Run this step."""
        self._start()
        try:
            self._execute()
        except Exception:
            self._fail()
        self._end()
        return self.resume()

    def to_thread(self):
        """Run this step in a separate thread."""
        logger.debug(f'Creating thread for {self} in {self.task}...')
        name = f'Step-{self.seqno}'
        self.thread = th.Thread(name=name, target=self.run, daemon=True)
        logger.debug(f'Created {self.thread.name} for {self} in {self.task}')
        self.pipeline.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def resume(self):
        """Proceed task execution in threads of following steps."""
        if self.status == 'D':
            for step in self.last.joins:
                step.to_thread()
        pass

    def _start(self):
        logger.info(f'Starting {self}...')
        self.start_date = dt.datetime.now()
        self.status = 'S'
        self.id = self.logger.root.table.primary_key
        if self.id is not None:
            table = self.logger.root.table.proxy
            select = f'SELECT * FROM {table} WHERE id = {self.id}'
            logger.debug(f'{self} DB query: {select}')
        self.logger.table(start_date=self.start_date,
                          job_id=self.job.id if self.job else None,
                          run_id=self.job.record_id if self.job else None,
                          task_id=self.pipeline.task.id,
                          step_name=self.name,
                          step_type=self.type,
                          step_date=self.pipeline.date)
        if self.a is not None:
            self.logger.table(step_a=self.a.node_name)
            if self.b is not None:
                self.logger.table(step_b=self.b.node_name)
                if self.c is not None:
                    self.logger.table(step_c=self.c.node_name)
        logger.debug(f'{self} started')
        return atexit.register(self._exit)

    def _execute(self):
        logger.info(f'{self} running')
        logger.info(f'{self} is {self.type} operation: {self.graph}')
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
        elif self.type == 'EX':
            self.a.prepare()
            self.a.to_executor(self)
        for thread in self.threads:
            logger.debug(f'Waiting for {thread.name} to finish...')
            thread.join()
            logger.debug(f'{thread.name} finished')
        pass

    def _fail(self):
        logger.error()
        return self.__fail()

    def __fail(self):
        exc_info = sys.exc_info()
        self.errors.add(exc_info)
        if self.pipeline:
            self.pipeline.task.errors.add(exc_info)
            if self.pipeline.job:
                self.pipeline.job.errors.add(exc_info)
        pass

    def _end(self):
        logger.debug(f'Ending {self}...')
        self.status = self._done() if not self.errors else self._error()
        if self.status == 'D':
            self.end_date = dt.datetime.now()
            self.logger.table(end_date=self.end_date)
        elif self.status == 'E':
            if self.pipeline.logging.step.fields.get('text_error'):
                self.logger.table(text_error=self.text_error)
        logger.debug(f'{self} ended')
        pass

    def _done(self):
        logger.info(f'{self} done')
        return 'D'

    def _error(self):
        logger.info(f'{self} failed')
        return 'E'

    def _exit(self):
        logger.debug(f'{self} exits...')
        if self.status is not None:
            if self.status not in ('D', 'E'):
                logger.debug(f'Caught unexpected end in {self}')
                self.status = 'U'
        pass

    pass


class Node(Unit):
    """Represents task node."""

    def __init__(self, node_name=None, source_name=None, pipeline=None):
        self.node_name = node_name or self.__class__.__name__
        self.source_name = source_name
        self.seqno = None
        self.thread = None
        if pipeline:
            self.setup(pipeline)

        self.steps = []
        self._prev = []
        self._next = []
        pass

    @property
    def node_name(self):
        """Get node name."""
        return self.name

    @node_name.setter
    def node_name(self, value):
        self.name = value
        pass

    @property
    def source_name(self):
        """Get data source name."""
        return self._source_name

    @source_name.setter
    def source_name(self, value):
        if isinstance(value, str) or value is None:
            self._source_name = value
            if self._source_name == 'localhost' or value is None:
                self.server = Localhost()
            else:
                connection = connector.receive(value)
                if isinstance(connection, Database):
                    self.database = self.db = connection
                elif isinstance(connection, Server):
                    self.server = connection
        pass

    @property
    def prev(self):
        """List all connected nodes that precede this one."""
        return self._prev

    @prev.setter
    def prev(self, node):
        """Connect the node as a preceding to this one."""
        if isinstance(node, Node) and node not in self._prev:
            self._prev.append(node)
            node.next = self
        pass

    @property
    def next(self):
        """List all connected nodes that proceed this one."""
        return self._next

    @next.setter
    def next(self, node):
        """Connect the node as a proceeding to this one."""
        if isinstance(node, Node) and node not in self._next:
            self._next.append(node)
        pass

    @property
    def joins(self):
        """List all steps where the node is a join."""
        return self.steps

    def setup(self, pipeline, root):
        """Configure the node using given pipeline."""
        self.pipeline = pipeline
        self.task = pipeline.task
        self.prev = root
        name = self.name
        seqno = 1
        while True:
            if self.pipeline.nodes.get(name) is None:
                self.renumber(seqno)
                if seqno > 1:
                    self.rename(name)
                break
            elif self.pipeline.nodes.get(name) == self:
                return
            else:
                seqno += 1
                name = f'{self.name}-{seqno}'
        self.pipeline.nodes[name] = self
        logger.debug(f'Node {self} configured in {pipeline}')
        pass

    def join(self, step):
        """Make this node a join between the given and following steps."""
        if isinstance(step, Step):
            if step not in self.steps:
                self.steps.append(step)
        else:
            message = (f'step must be Step, not {step.__class__.__name__}')
            raise TypeError(message)
        pass

    pass
