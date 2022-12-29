"""Contains application core elements."""

import argparse
import atexit
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
from .utils import declare, cache, terminator
from .utils import case, coalesce
from .utils import first, last
from .utils import to_boolean
from .utils import to_datetime, to_timestamp
from .utils import to_lower, to_upper
from .utils import to_thread, to_process, to_python
from .utils import sql_preparer
from .const import LINUX, MACOS, WINDOWS

from .config import Logging
from .config import Localhost, Server, Database


class Scheduler():
    """Represents application scheduler."""

    def __init__(self, name=None, desc=None, owner=None):
        self.path = locate()

        self.conn = db.connect()
        self.logger = logger

        self.name = name
        self.desc = desc
        self.owner = owner

        self.moment = tm.time()
        self.schedule = None

        self.queue = queue.Queue()
        self.entry_queue = queue.Queue()
        self.running_lists = {}
        self.failure_lists = {}
        self.waiting_lists = {}

        self.reading = th.Event()
        self.resurrection = th.Event()
        self.waking_up = th.Event()

        self.procs = {}
        self.daemons = []
        self.chargers = []
        self.executors = []

        self.server_name = platform.node()
        self.user_name = os.getlogin()
        self.pid = os.getpid()
        self.start_date = None
        self.stop_date = None
        self.status = None

        logger.configure(format='{isodate}\t{thread}\t{rectype}\t{message}\n',
                         alarming=False)

        args = self._parse_arguments()
        argv = [arg for arg in sys.argv if arg.startswith('-') is False]
        if len(argv) > 1:
            arg = argv[1]
            if arg == 'start':
                path = os.path.abspath(sys.argv[0])
                to_python(path, '--start')
            elif arg == 'stop':
                logger.configure(file=False, debug=args.debug)
                self.stop()
        else:
            if args.start:
                self.start()
            elif args.stop:
                logger.configure(file=False, debug=args.debug)
                self.stop()
        pass

    def __repr__(self):
        """Represent this scheduler with its name."""
        return self.name

    class Job:
        """Represents scheduler job."""

        def __init__(self, id, scheduler, name=None, desc=None, status=None,
                     mday=None, hour=None, min=None, sec=None,
                     wday=None, yday=None, trigger_id=None,
                     start_timestamp=None, end_timestamp=None,
                     env=None, args=None, timeout=None, parallelism=None,
                     rerun_interval=None, rerun_period=None, rerun_done=None,
                     rerun_times=None, rerun_limit=None, rerun_days=None,
                     sleep_period=None, wake_up_period=None,
                     tag=None, record_id=None, added=None, deactivated=None):
            self.id, self.scheduler = id, scheduler
            self.setup(name=name, desc=desc, status=status,
                       mday=mday, hour=hour, min=min, sec=sec,
                       wday=wday, yday=yday, trigger_id=trigger_id,
                       start_timestamp=start_timestamp,
                       end_timestamp=end_timestamp,
                       env=env, args=args,
                       timeout=timeout, parallelism=parallelism,
                       rerun_interval=rerun_interval,
                       rerun_period=rerun_period,
                       rerun_done=rerun_done,
                       rerun_times=rerun_times, rerun_limit=rerun_limit,
                       rerun_days=rerun_days,
                       sleep_period=sleep_period,
                       wake_up_period=wake_up_period,
                       tag=tag, record_id=record_id,
                       added=added, deactivated=deactivated)

        def __repr__(self):
            if self.is_run:
                if self.record_id:
                    return f'Job[{self.id}:{self.tag}:{self.record_id}]'
                else:
                    return f'Job[{self.id}:{self.tag}]'
            else:
                return f'Job[{self.id}]'

        @property
        def is_schedule(self):
            """Determine if this job is a schedule entity."""
            if self.tag and self.record_id:
                return False
            else:
                return True

        @property
        def is_run(self):
            """Determine if this job is a run entity."""
            return not self.is_schedule

        @property
        def is_regular(self):
            """Check if the job is a regular type."""
            if not self.trigger_id:
                return True
            else:
                return False

        @property
        def is_child(self):
            """Check if the job is a child of some other job."""
            if self.trigger_id:
                return True
            else:
                return False

        @property
        def done(self):
            """Check wheter job is done or not."""
            if self.is_run:
                if self.rerun_done:
                    return True
                else:
                    return False

        @property
        def config(self):
            """Get configuration fields."""
            return types.SimpleNamespace(
                name=self.name,
                desc=self.desc,
                status=self.status,
                mday=self.mday,
                hour=self.hour,
                min=self.min,
                sec=self.sec,
                wday=self.wday,
                yday=self.yday,
                trigger_id=self.trigger_id,
                start_timestamp=self.start_timestamp,
                end_timestamp=self.end_timestamp,
                env=self.env,
                args=self.args,
                timeout=self.timeout,
                parallelism=self.parallelism,
                rerun_interval=self.rerun_interval,
                rerun_limit=self.rerun_limit,
                rerun_days=self.rerun_days,
                rerun_period=self.rerun_period,
                sleep_period=self.sleep_period,
                wake_up_period=self.wake_up_period
            )

        def setup(self, name=None, desc=None, status=None,
                  mday=None, hour=None, min=None, sec=None,
                  wday=None, yday=None, trigger_id=None,
                  start_timestamp=None, end_timestamp=None,
                  env=None, args=None, timeout=None, parallelism=None,
                  rerun_interval=None, rerun_period=None, rerun_done=None,
                  rerun_times=None, rerun_limit=None, rerun_days=None,
                  sleep_period=None, wake_up_period=None,
                  tag=None, record_id=None, added=None, deactivated=None):
            """Configure this job."""
            self.name = name
            self.desc = desc
            self.status = to_boolean(status)
            self.mday = mday
            self.hour = hour
            self.min = min
            self.sec = sec
            self.wday = wday
            self.yday = yday
            self.trigger_id = trigger_id
            self.start_timestamp = to_timestamp(start_timestamp)
            self.end_timestamp = to_timestamp(end_timestamp)
            self.env = env
            self.args = args
            self.timeout = timeout
            self.parallelism = parallelism
            self.sleep_period = sleep_period
            self.wake_up_period = wake_up_period

            self.tag = tag
            self.record_id = record_id
            self.added = to_timestamp(added)
            self.deactivated = to_timestamp(deactivated)

            self.rerun_interval = rerun_interval
            self.rerun_period = rerun_period
            self.rerun_done = to_boolean(rerun_done)
            self.rerun_times = coalesce(rerun_times, 0)
            self.rerun_limit = coalesce(rerun_limit, 0)
            self.rerun_days = coalesce(rerun_days, 0)

        def check_in(self):
            """Put this job into scheduler running list."""
            if not self.scheduler.running_lists.get(self.id):
                self.scheduler.running_lists[self.id] = {}
            running_list = self.scheduler.running_lists[self.id]
            running_list[self.record_id] = [self, self.tag]

        def check_out(self):
            """Pop this job from the scheduler running list."""
            if self.scheduler.running_lists[self.id]:
                running_list = self.scheduler.running_lists[self.id]
                running_list.pop(self.record_id)

        def for_rerun(self):
            """Put this job into failure list."""
            if not self.scheduler.failure_lists.get(self.id):
                self.scheduler.failure_lists[self.id] = {}
            failure_list = self.scheduler.failure_lists[self.id]
            if not failure_list.get(self.record_id):
                failure_list[self.record_id] = [self, self.tag]

        def for_sleep(self):
            """Put this job to sleep."""
            if not self.scheduler.waiting_lists.get(self.id):
                self.scheduler.waiting_lists[self.id] = {}
            waiting_list = self.scheduler.waiting_lists[self.id]
            if not waiting_list.get(self.record_id):
                waiting_list[self.record_id] = [self, self.tag]

        def from_schedule(self, record):
            """Configure this simple job using schedule record."""
            self.setup(name=record.job_name,
                       desc=record.job_description,
                       status=record.status,
                       mday=record.monthday,
                       hour=record.hour,
                       min=record.minute,
                       sec=record.second,
                       wday=record.weekday,
                       yday=record.yearday,
                       trigger_id=record.trigger_id,
                       start_timestamp=record.start_date,
                       end_timestamp=record.end_date,
                       env=record.environment,
                       args=record.arguments,
                       timeout=record.timeout,
                       parallelism=record.parallelism,
                       rerun_interval=record.rerun_interval,
                       rerun_limit=record.rerun_limit,
                       rerun_days=record.rerun_days,
                       rerun_period=record.rerun_period,
                       sleep_period=record.sleep_period,
                       wake_up_period=record.wake_up_period)
            return self

        def from_run(self, record):
            """Configure this simple job using run record."""
            self.setup(tag=record.run_tag,
                       record_id=record.id,
                       name=record.job_name,
                       desc=record.job_description,
                       status=record.status,
                       mday=record.monthday,
                       hour=record.hour,
                       min=record.minute,
                       sec=record.second,
                       wday=record.weekday,
                       yday=record.yearday,
                       trigger_id=record.trigger_id,
                       added=record.added,
                       start_timestamp=record.start_date,
                       end_timestamp=record.end_date,
                       env=record.environment,
                       args=record.arguments,
                       timeout=record.timeout,
                       parallelism=record.parallelism,
                       rerun_interval=record.rerun_interval,
                       rerun_period=record.rerun_period,
                       rerun_done=record.rerun_done,
                       rerun_times=record.rerun_times,
                       rerun_limit=record.rerun_limit,
                       rerun_days=record.rerun_days,
                       sleep_period=record.sleep_period,
                       wake_up_period=record.wake_up_period,
                       deactivated=record.deactivated)
            return self

        def to_run(self, tag, record_id):
            """Convert this simple job into run."""
            parameters = self.config.__dict__
            return self.__class__(self.id, self.scheduler, **parameters,
                                  tag=tag, record_id=record_id)

        def refresh_config(self):
            """Refresh configuration fields."""
            record = self.scheduler.get_job(self.id)
            for key in self.config.__dict__.keys():
                setattr(self, key, record.__dict__[key])

        def refresh_process(self):
            """Refresh process fields."""
            if self.is_run:
                conn = db.connect()
                table = db.tables.run_history
                select = table.select().where(table.c.id == self.record_id)
                result = conn.execute(select).first()
                self.rerun_done = to_boolean(result.rerun_done)
                self.rerun_times = coalesce(result.rerun_times, 0)
                self.deactivated = to_timestamp(result.deactivated)

        def is_active(self):
            """Check if this job is active for the current timestamp."""
            return self.was_active(self.scheduler.moment)

        def was_active(self, timestamp):
            """Check if this job was active for a given timestamp."""
            if (
                self.status is True
                and coalesce(self.start_timestamp, timestamp-1) < timestamp
                and coalesce(self.end_timestamp, timestamp+1) > timestamp
            ):
                return True
            else:
                return False

        def is_scheduled(self):
            """Check if this job is scheduled at the current timestamp."""
            return self.was_scheduled(self.scheduler.moment)

        def was_scheduled(self, timestamp):
            """Check if this job was scheduled at a given timestamp."""
            if self.is_regular:
                s = self.scheduler.parser(timestamp)
                if (
                    self.scheduler.matcher(self.mday, s.mday)
                    and self.scheduler.matcher(self.hour, s.hour)
                    and self.scheduler.matcher(self.min, s.min)
                    and self.scheduler.matcher(self.sec, s.sec)
                    and self.scheduler.matcher(self.wday, s.wday)
                    and self.scheduler.matcher(self.yday, s.yday)
                ):
                    return True
                else:
                    return False
            else:
                return False

        def is_sleeping(self):
            """Check if the sleep window is currently active."""
            return self.was_sleeping(self.scheduler.moment)

        def was_sleeping(self, timestamp):
            """Check if the sleep window was active at a timestamp."""
            if self.sleep_period:
                s = self.scheduler.parser(timestamp)
                if self.scheduler.matcher(self.sleep_period, s.hour):
                    return True
                else:
                    if self.wake_up_period:
                        if self.scheduler.matcher(self.wake_up_period, s.min):
                            return False
                        else:
                            return True
                    else:
                        return False
            else:
                return False

        def is_waiting(self):
            """Check if this job has to wait."""
            if self.scheduler.waiting_lists.get(self.id):
                return True
            else:
                return False

        def is_busy(self):
            """Check if the given scheduler is free to run this job."""
            if self.parallelism:
                parameter = self.parallelism
                limit = case(parameter, 'Y', 999, 'N', 1, parameter)
                if isinstance(limit, str):
                    if limit.isdigit():
                        limit = int(limit)
                    else:
                        limit = 1
                if isinstance(limit, int):
                    if self.scheduler.count_running(self.id) >= limit:
                        return True
                    else:
                        return False
            else:
                return False

        def is_ready(self):
            """Check if this job is ready for run or rerun."""
            if (
                not self.is_sleeping()
                and not self.is_waiting()
                and not self.is_busy()
            ):
                return True
            else:
                return False

        def is_awaken(self):
            """Check if this job is ready to awake."""
            if (
                not self.is_sleeping()
                and not self.is_busy()
            ):
                return True
            else:
                return False

        def is_rerun_time(self):
            """Check if rerun time has come."""
            return self.was_rerun_time(self.scheduler.moment)

        def was_rerun_time(self, timestamp):
            """Check if rerun time had came at a timestamp."""
            if self.rerun_interval and timestamp % self.rerun_interval == 0:
                s = self.scheduler.parser(timestamp)
                if self.rerun_period:
                    if self.scheduler.matcher(self.rerun_period, s.min):
                        return True
                    else:
                        return False
                else:
                    return True

        def is_rerun_available(self):
            """Check if rerun is currently available."""
            return self.was_rerun_available(self.scheduler.moment)

        def was_rerun_available(self, timestamp):
            """Check if rerun was available at a timestamp."""
            if self.rerun_interval:
                now = dt.datetime.fromtimestamp(timestamp)
                delay = config['SCHEDULER']['rerun_delay']
                interval = coalesce(self.rerun_interval, 0)
                date_from = now-dt.timedelta(days=self.rerun_days)
                date_to = now-dt.timedelta(seconds=delay+interval)
                if (
                    self.rerun_times < self.rerun_limit
                    and to_datetime(self.added) > date_from
                    and to_datetime(self.added) < date_to
                ):
                    return True
                else:
                    return False
            else:
                return False

        pass

    @property
    def running(self):
        """Check whether scheduler running or not."""
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'SCHEDULER')
        result = conn.execute(select).first()
        status = True if result.status == 'Y' else False
        return status

    @property
    def current_timestamp(self):
        """Get current scheduler timestamp."""
        return int(self.moment) if self.moment else 0

    @property
    def current_datetime(self):
        """Get current scheduler datetime."""
        return dt.datetime.fromtimestamp(self.current_timestamp)

    def start(self):
        """Launch the scheduler."""
        if self.running is False:
            self._start()
            # Iterate scheduler process.
            while True:
                self._process()
        else:
            raise Exception('scheduler already running')

    def stop(self):
        """Stop the scheduler."""
        self._terminate()

    def restart(self):
        """Restart the scheduler."""
        self.stop()
        self.start()

    def read(self):
        """Initiate the schedule reading."""
        if not self.reading.is_set():
            return self.reading.set()

    def rerun(self):
        """Initiate launch procedure of failed jobs."""
        if not self.resurrection.is_set():
            return self.resurrection.set()

    def wake_up(self):
        """Initiate launch procedure of sleeping jobs."""
        if not self.waking_up.is_set():
            return self.waking_up.set()

    def count_running(self, id):
        """Get the number of currently running jobs."""
        if self.running_lists.get(id):
            return len(self.running_lists[id])
        else:
            return 0

    def _count_running(self, id):
        conn = db.connect()
        table = db.tables.run_history
        select = sa.select(sa.func.count()).select_from(table).where(
            sa.and_(
                table.c.job_id == id,
                table.c.status == 'R'
            )
        )
        select = select.where(table.c.job_id == id)
        result = conn.execute(select).scalar()
        return result

    def count_failed(self):
        """Get the total number of failed jobs."""
        h = db.tables.run_history
        select = (sa.select([sa.func.count()]).select_from(h).
                  where(sa.and_(h.c.status.in_(['E', 'T']),
                                h.c.rerun_id.is_(None),
                                h.c.rerun_now.is_(None),
                                h.c.rerun_done.is_(None))))
        conn = db.connect()
        result = conn.execute(select).scalar()
        return result

    def count_sleeping(self):
        """Get the current number of sleeping jobs."""
        result = sum((len(jobs) for jobs in self.waiting_lists.values()))
        return result

    def get_job(self, id):
        if self.schedule:
            return [record for record in self.schedule if record.id == id][0]

    def list_jobs(self):
        """Get the list of all scheduled jobs."""
        conn = db.connect()
        table = db.tables.schedule
        select = table.select().order_by(table.c.id)
        result = conn.execute(select).fetchall()
        for record in result:
            job = self.Job(record.id, self).from_schedule(record)
            yield job

    def list_failed_jobs(self):
        """Get the list of the failed jobs."""
        h = db.tables.run_history
        s = db.tables.schedule
        select = (sa.select([h.c.id, h.c.job_id, h.c.run_tag, h.c.added,
                             s.c.job_name, s.c.job_description, s.c.status,
                             s.c.hour, s.c.minute, s.c.second,
                             s.c.yearday, s.c.monthday, s.c.weekday,
                             s.c.trigger_id, s.c.start_date, s.c.end_date,
                             s.c.environment, s.c.arguments,
                             s.c.timeout, s.c.parallelism,
                             s.c.rerun_interval, s.c.rerun_period,
                             s.c.sleep_period, s.c.wake_up_period,
                             h.c.rerun_times, s.c.rerun_limit, s.c.rerun_days,
                             h.c.rerun_done, h.c.deactivated]).
                  select_from(sa.join(h, s, h.c.job_id == s.c.id)).
                  where(sa.and_(h.c.status.in_(['E', 'T']),
                                h.c.rerun_id.is_(None),
                                h.c.rerun_now.is_(None),
                                h.c.rerun_done.is_(None))).
                  order_by(h.c.id))
        conn = db.connect()
        result = conn.execute(select).fetchall()
        for record in result:
            job = self.Job(record.job_id, self).from_run(record)
            tag = record.run_tag
            yield job, tag

    def list_sleeping_jobs(self):
        """Get the list of the sleeping jobs."""
        h = db.tables.run_history
        s = db.tables.schedule
        select = (sa.select([h.c.id, h.c.job_id, h.c.run_tag, h.c.added,
                             s.c.job_name, s.c.job_description, s.c.status,
                             s.c.hour, s.c.minute, s.c.second,
                             s.c.yearday, s.c.monthday, s.c.weekday,
                             s.c.trigger_id, s.c.start_date, s.c.end_date,
                             s.c.environment, s.c.arguments,
                             s.c.timeout, s.c.parallelism,
                             s.c.rerun_interval, s.c.rerun_period,
                             s.c.sleep_period, s.c.wake_up_period,
                             h.c.rerun_times, s.c.rerun_limit, s.c.rerun_days,
                             h.c.rerun_done, h.c.deactivated]).
                  select_from(sa.join(h, s, h.c.job_id == s.c.id)).
                  where(sa.and_(h.c.status == 'W')).
                  order_by(h.c.id))
        conn = db.connect()
        result = conn.execute(select).fetchall()
        for record in result:
            job = self.Job(record.job_id, self).from_run(record)
            tag = record.run_tag
            yield job, tag

    def parser(self, timestamp):
        """Parse the given timestamp into the desired structure."""
        structure = tm.localtime(timestamp)
        namespace = types.SimpleNamespace(
            timestamp=timestamp,
            mday=structure.tm_mday,
            hour=structure.tm_hour,
            min=structure.tm_min,
            sec=structure.tm_sec,
            wday=structure.tm_wday+1,
            yday=structure.tm_yday
        )
        return namespace

    def matcher(self, period, unit):
        """Match the given period parameter and time unit."""
        # Check if empty or *.
        if period is None or re.match(r'^(\*)$', period) is not None:
            return True
        # Check if period is lonely digit and equals to unit.
        elif re.match(r'^\d+$', period) is not None:
            period = int(period)
            return True if unit == period else False
        # Check if period is a cycle and integer division with unit is true.
        elif re.match(r'^/\d+$', period) is not None:
            period = int(re.search(r'\d+', period).group())
            if period == 0:
                return False
            return True if unit % period == 0 else False
        # Check if period is a range and unit is in this range.
        elif re.match(r'^\d+-\d+$', period) is not None:
            period = [int(i) for i in re.findall(r'\d+', period)]
            return True if unit in range(period[0], period[1]+1) else False
        # Check if period is a start value for unit.
        elif re.match(r'^\d+\+$', period) is not None:
            period = int(re.search(r'\d+', period).group())
            return True if unit >= period else False
        # Check if period is a list and unit is in this list.
        elif re.match(r'^\d+,\s*\d+.*$', period):
            period = [int(i) for i in re.findall(r'\d+', period)]
            return True if unit in period else False
        # All other cases is not for the unit.
        else:
            return False

    def _start(self):
        # Perform all necessary preparations before scheduler start.
        logger.head()
        logger.info('Starting scheduler...')
        self.status = True
        self.start_date = dt.datetime.now()
        self._set_signals()
        self._make_threads()
        self._update_component()
        self._read_schedule()
        self._read_failed_jobs()
        self._read_sleeping_jobs()
        self._synchronize()
        logger.info(f'Scheduler started on PID[{self.pid}]')

    def _process(self):
        # Perform scheduler phases.
        self._active()
        self._passive()

    def _shutdown(self, signum=None, frame=None):
        # Stop this running scheduler.
        logger.info('Stopping scheduler...')
        self._stop()
        logger.info(f'Scheduler on PID[{self.pid}] stopped')
        self._exit()

    def _stop(self):
        # Perform all necessary actions before scheduler stop.
        self.status = False
        self.stop_date = dt.datetime.now()
        self._update_component()

    def _exit(self):
        # Terminate the processes of the scheduler and its running jobs.
        if WINDOWS:
            for pid in self.procs:
                try:
                    logger.debug(f'Terminating Job on PID[{pid}]')
                    terminator(pid)
                except (OSError, ProcessLookupError):
                    logger.warning(f'Job on PID[{pid}] not found')
                except Exception:
                    logger.error()
        sys.exit()

    def _terminate(self):
        # Kill running scheduler.
        logger.debug('Killing the scheduler...')
        conn = db.connect()
        table = db.tables.components
        select = table.select().where(table.c.id == 'SCHEDULER')
        result = conn.execute(select).first()
        status = to_boolean(result.status)
        pid = result.pid
        logger.debug(f'Scheduler should be on PID[{pid}]')
        try:
            terminator(pid)
        except (OSError, ProcessLookupError):
            logger.warning(f'Scheduler on PID[{pid}] not found')
            if status:
                self._stop()
        except Exception:
            logger.error()
        else:
            logger.debug(f'Scheduler on PID[{pid}] successfully terminated')

    def _active(self):
        # Run active scheduler phase.
        self._maintenance()
        self._walk()

    def _passive(self):
        # Run passive scheduler phase.
        self._timeshift()

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

    def _synchronize(self):
        # Synchronize the internal time with the real one.
        logger.debug('Time will be synchronized')
        self.moment = int(tm.time())
        logger.debug('Time synchronized')

    def _increment(self):
        # Move scheduler internal tm.
        self.moment += 1

    def _maintenance(self):
        # Perform maintenance steps.
        # Update schedule if needed.
        interval = config['SCHEDULER']['refresh_interval']
        if int(self.moment) % interval == 0:
            logger.debug('Schedule refresh procedure initiates right now')
            self.read()
        # Rerun failed jobs.
        interval = config['SCHEDULER']['rerun_interval']
        if int(self.moment) % interval == 0:
            logger.debug('Rerun procedure initiates right now')
            self.rerun()
        # Finalize postponed jobs.
        interval = config['SCHEDULER']['wakeup_interval']
        if int(self.moment) % interval == 0:
            logger.debug('Wake Up procedure initiates right now')
            self.wake_up()

    def _read(self):
        return self._read_schedule()

    def _read_schedule(self):
        # Parse the schedule from the table into memory.
        logger.debug('Refreshing schedule...')
        self.schedule = list(self._sked())
        if self.schedule:
            logger.debug(f'Schedule refreshed {self.schedule}')
        else:
            logger.debug('Schedule is empty')

    def _read_failed_jobs(self):
        # Parse the list of failed jobs from the history into memory.
        logger.debug('Preparing a list of failed jobs')
        n = 0
        found_jobs = []
        failed_jobs = self.list_failed_jobs()
        for job, tag in failed_jobs:
            if job.is_rerun_available():
                job.for_rerun()
                n += 1
                found_jobs.append(job)
        logger.debug(f'Found {n} failed jobs: {found_jobs}')
        logger.debug('List of failed jobs prepared')

    def _read_sleeping_jobs(self):
        # Parse the list of sleeping jobs from the history into memory.
        logger.debug('Preparing a list of sleeping jobs')
        n = 0
        found_jobs = []
        sleeping_jobs = self.list_sleeping_jobs()
        for job, tag in sleeping_jobs:
            job.for_sleep()
            n += 1
            found_jobs.append(job)
        logger.debug(f'Found {n} sleeping jobs: {found_jobs}')
        logger.debug(f'List of sleeping jobs prepared')

    def _sked(self):
        # Read and parse the schedule from the database table.
        logger.debug('Getting schedule...')
        for job in self.list_jobs():
            try:
                yield job
            except Exception:
                logger.warning()
                continue
        logger.debug('Schedule retrieved')

    def _walk(self):
        # Walk through the jobs and find out which must be executed.
        for job in self.schedule:
            try:
                if job.is_active() and job.is_scheduled():
                    self.entry_queue.put((job, self.moment))
            except Exception:
                logger.error()

    def _reader(self):
        # Read and update in-memory schedule if necessary.
        while True:
            if self.reading.is_set():
                try:
                    self._read()
                except Exception:
                    logger.error()
                finally:
                    self.reading.clear()
            tm.sleep(1)

    def _rerun(self):
        # Define failed runs and send them on re-execution.
        while True:
            if self.resurrection.is_set() and self.count_failed():
                logger.debug('Rerun procedure starts...')
                n = 0
                found_jobs = []
                failed_jobs = self.list_failed_jobs()
                for job, tag in failed_jobs:
                    if job.is_rerun_time() and job.is_rerun_available():
                        job.for_rerun()
                        n += 1
                        found_jobs.append(job)
                logger.debug(f'Found {n} failed jobs: {found_jobs}')
                for queue in self.failure_lists.values():
                    if queue:
                        job, tag = first(queue)
                        self._regain_failed_job(job, tag)
                logger.debug('Rerun procedure completed')
                self.resurrection.clear()
            tm.sleep(1)

    def _wake_up(self):
        # Define runs to be woken up from sleep and then executed.
        while True:
            if self.waking_up.is_set() and self.count_sleeping():
                logger.debug('Wake Up procedure starts...')
                for queue in self.waiting_lists.values():
                    if queue:
                        job, tag = first(queue)
                        self._regain_sleeping_job(job, tag)
                logger.debug('Wake Up procedure completed')
                self.waking_up.clear()
            tm.sleep(1)

    def _charger(self):
        # Add jobs from the entry queue to the execution queue.
        while True:
            if not self.entry_queue.empty():
                job, tag = self.entry_queue.get()
                logger.debug(f'Thread used by {job}')
                if job.is_sleeping():
                    logger.debug(f'{job} waits due to sleep window')
                    self._postpone(job, tag)
                elif job.is_waiting():
                    logger.debug(f'{job} waits due to waiting runs')
                    self._postpone(job, tag)
                elif job.is_busy():
                    logger.debug(f'{job} waits due to parallelism')
                    self._postpone(job, tag)
                else:
                    logger.debug(f'{job} is ready to execute')
                    self._charge(job, tag)
                self.entry_queue.task_done()
                logger.debug(f'Thread released from {job}')
            tm.sleep(1)

    def _charge(self, job: Job, tag: int):
        # Add job to the execution queue.
        try:
            date = dt.datetime.fromtimestamp(tag)
            logger.info(f'Adding {job} to the queue from schedule...')
            logger.debug(f'Creating new run history record for {job}...')
            conn = db.connect()
            table = db.tables.run_history
            insert = (table.insert().
                      values(job_id=job.id,
                             run_tag=tag,
                             run_date=date,
                             added=dt.datetime.now(),
                             status='Q',
                             server_name=self.server_name,
                             user_name=self.user_name))
            result = conn.execute(insert)
            record_id = result.inserted_primary_key[0]
            logger.debug(f'Created run history record {record_id} for {job}')
            job = job.to_run(tag, record_id)
            job.check_in()
            self.queue.put((job, tag))
        except Exception:
            logger.error()
        else:
            logger.info(f'{job} added to the queue from schedule')

    def _postpone(self, job: Job, tag: int):
        try:
            date = dt.datetime.fromtimestamp(tag)
            logger.info(f'Postponing {job}...')
            logger.debug(f'Creating new run history record for {job}...')
            conn = db.connect()
            table = db.tables.run_history
            insert = (table.insert().
                      values(job_id=job.id,
                             run_tag=tag,
                             run_date=date,
                             added=dt.datetime.now(),
                             status='W',
                             server_name=self.server_name,
                             user_name=self.user_name))
            result = conn.execute(insert)
            record_id = result.inserted_primary_key[0]
            logger.debug(f'Created run history record {record_id} for {job}')
            job = job.to_run(tag, record_id)
            job.for_sleep()
        except Exception:
            logger.error()
        else:
            logger.info(f'{job} postponed')

    def _executor(self):
        # Execute jobs registered in the execution queue.
        while True:
            if not self.queue.empty():
                job, tag = self.queue.get()
                logger.debug(f'Thread used by {job}')
                logger.debug(f'{job} will be executed now using {job.config}')
                self._execute(job, tag)
                self.queue.task_done()
                job.check_out()
                logger.debug(f'Thread released from {job}')
            tm.sleep(1)

    def _execute(self, job: Job, tag: int):
        try:
            env = job.env or 'python'
            args = job.args or ''
            timeout = job.timeout
            record_id = job.record_id
            logger.info(f'Initiating {job}...')
            exe = config['ENVIRONMENTS'].get(env)
            path = os.path.join(self.path, f'jobs/{job.id}/job.py')
            args += f' run -a --record {record_id}'
            proc = to_process(exe, path, args)
            logger.info(f'{job} runs on PID {proc.pid}')
            logger.debug(f'Waiting for {job} to finish...')
            self.procs[proc.pid] = proc
            proc.wait(timeout)
        except sp.TimeoutExpired:
            logger.warning(f'{job} timeout exceeded')
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
            self.procs.pop(proc.pid)
            if proc.returncode > 0:
                logger.info(F'{job} completed with error')
                try:
                    conn = db.connect()
                    table = db.tables.run_history
                    text_error = proc.stderr.read().decode()
                    update = (table.update().
                              values(status='E', error_list=text_error).
                              where(table.c.id == record_id))
                    conn.execute(update)
                except Exception:
                    logger.error()
            else:
                logger.info(f'{job} completed')

    def _regain_failed_job(self, job: Job, tag: int):
        try:
            job.refresh_config()
            job.refresh_process()
            logger.debug(f'Checking {job} for rerun using {job.config}')
            if not job.done and not job.deactivated:
                if job.was_active(tag) and job.is_ready():
                    logger.debug(f'{job} is ready for rerun')
                    logger.info(f'Adding {job} to the queue for rerun...')
                    job.check_in()
                    self.queue.put((job, tag))
                    logger.info(f'{job} added to the queue for rerun')
                    self.failure_lists[job.id].pop(job.record_id)
                else:
                    logger.debug(f'{job} is not ready for rerun')
            else:
                logger.debug(f'{job} rerun already done or deactivated')
                self.failure_lists[job.id].pop(job.record_id)
        except Exception:
            logger.error()

    def _regain_sleeping_job(self, job: Job, tag: int):
        try:
            job.refresh_config()
            logger.debug(f'Checking {job} for awake using {job.config}')
            if job.is_awaken():
                logger.debug(f'{job} is ready to awake')
                logger.info(f'Adding {job} to the queue from sleep...')
                job.check_in()
                self.queue.put((job, tag))
                logger.info(f'{job} added to the queue from sleep')
                self.waiting_lists[job.id].pop(job.record_id)
        except Exception:
            logger.error()

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', action='store_true',
                            required=False, help='start new scheduler')
        parser.add_argument('--stop', action='store_true',
                            required=False, help='stop running scheduler')
        parser.add_argument('--debug', action='store_true',
                            required=False, help='enable debug mode')
        args, anons = parser.parse_known_args()
        return args

    def _set_signals(self):
        # Configure signal triggers.
        logger.debug('Setting signal triggers...')
        signal.signal(signal.SIGINT, self._shutdown)
        logger.debug(f'SIGINT trigger for PID[{self.pid}] set')
        signal.signal(signal.SIGTERM, self._shutdown)
        logger.debug(f'SIGTERM trigger for PID[{self.pid}] set')

    def _make_threads(self):
        # Configure all necessary threads.
        logger.debug('Making threads for this scheduler...')

        scheduling = to_thread('Scheduling', self._reader)
        error_handler = to_thread('ErrorHandler', self._rerun)
        wait_control = to_thread('WaitControl', self._wake_up)
        for thread in [scheduling, error_handler, wait_control]:
            thread.start()
            self.daemons.append(thread)
        number = len(self.daemons)
        logger.debug(f'{number} maintaining daemons made {self.daemons}')

        number = config['SCHEDULER']['chargers_number']
        target = self._charger
        for i in range(number):
            name = f'Charger-{i}'
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.chargers.append(thread)
        logger.debug(f'{number} chargers made {self.chargers}')

        number = config['SCHEDULER']['executors_number']
        target = self._executor
        for i in range(number):
            name = f'Executor-{i}'
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.executors.append(thread)
        logger.debug(f'{number} executors made {self.executors}')

    def _update_component(self):
        # Update component information in database table.
        logger.debug('Updating component information...')
        conn = db.connect()
        table = db.tables.components
        update = table.update().where(table.c.id == 'SCHEDULER')
        if self.status is True:
            update = update.values(status='Y',
                                   start_date=self.start_date,
                                   stop_date=None,
                                   server_name=self.server_name,
                                   user_name=self.user_name,
                                   pid=self.pid)
        elif self.status is False:
            update = update.values(status='N',
                                   stop_date=self.stop_date)
        conn.execute(update)
        logger.debug('Component information updated')


class Job():
    """Represents single application job."""

    def __init__(self, id=None, name=None, desc=None, tag=None, date=None,
                 record_id=None, trigger_id=None, email_list=None,
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
        self.target = None

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
                self.initiator_id = result.rerun_id
                self.trigger_id = result.trigger_id
                if result.data_dump:
                    self.data = pickle.loads(result.data_dump)
                else:
                    self.data = types.SimpleNamespace()
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
            self.initiator_id = None
            self.trigger_id = coalesce(args.trigger, trigger_id)
            self.data = types.SimpleNamespace()
        self.errors = set()

        self.recycle_ind = 'Y' if self.args.recycle else None
        self.initiator = db.record(history, self.initiator_id)
        self.trigger = db.record(history, self.trigger_id)

        # Configure dependent objects.
        email_list = coalesce(email_list, schedule['email_list'])
        self.email_list = self._parse_email_list(email_list)

        self.auto = args.auto
        self.solo = args.solo
        self.alarm = coalesce(args.mute, alarm, schedule['alarm'], False)
        self.debug = coalesce(args.debug, debug, schedule['debug'], False)
        logger.configure(app=self.name, desc=self.desc,
                         debug=self.debug, alarming=self.alarm,
                         smtp={'recipients': self.email_list})

        self.server_name = platform.node()
        self.user_name = os.getlogin()
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
                self.target = 'recycle' if self.args.recycle else 'run'
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
        """Get textual representation of all found errors."""
        if self.errors:
            text_list = []
            for err_type, err_value, err_tb in self.errors:
                exception = tb.format_exception(err_type, err_value, err_tb)
                text = ''.join(exception)
                text_list.append(text)
            return f'{"":->34}\n'.join(text_list)
        else:
            return None

    @property
    def pipeline(self):
        """Build pipeline for this job if configured."""
        if self.configured:
            nodes = []
            models = imp.import_module('pydin.models')
            fields = imp.import_module('pydin.fields')
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

                key_name = to_lower(record['key_field'])
                key_field = getattr(fields, key_name) if key_name else None

                chunk_size = record['chunk_size']
                cleanup = to_boolean(record['cleanup'])

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
            settings = self.settings
            name = settings['pipeline_name']
            error_limit = settings['error_limit']
            sql_logging = to_boolean(settings['sql_logging'])
            file_logging = to_boolean(settings['file_logging'])
            logging = Logging(sql=sql_logging, file=file_logging)
            return Pipeline(*nodes, name=name, error_limit=error_limit,
                            logging=logging)

    @property
    def settings(self):
        """Get job pipeline settings."""
        conn = db.connect()
        p = db.tables.pipelines
        select = p.select().where(p.c.job_id == self.id)
        result = conn.execute(select).first()
        if result:
            return dict(result)

    @property
    def configuration(self):
        """Get job pipeline configuration."""
        conn = db.connect()
        c = db.tables.config
        select = c.select().where(c.c.job_id == self.id).\
                            order_by(c.c.node_seqno)
        result = conn.execute(select)
        if result:
            return [dict(record) for record in result]

    @property
    def configured(self):
        """Check if job configured."""
        return True if self.configuration else False

    @classmethod
    def get(cls):
        """Get existing job from current execution."""
        cache = imp.import_module('pydin.cache')
        object = getattr(cache, cls.__name__.lower())
        return object

    @classmethod
    def exists(cls):
        """Check if job exists in current execution."""
        cache = imp.import_module('pydin.cache')
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
            if self.status in ('Q', 'W'):
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
                          server_name=self.server_name,
                          user_name=self.user_name,
                          pid=self.pid,
                          rerun_id=self.initiator_id,
                          rerun_seqno=self.seqno,
                          recycle_ind=self.recycle_ind,
                          trigger_id=self.trigger_id,
                          file_log=self.file_log)
        pass

    def _start_as_continue(self):
        logger.debug(f'{self} will be executed as continue')

        self.record.write(run_mode=self.mode,
                          start_date=self.start_date,
                          status=self.status,
                          server_name=self.server_name,
                          user_name=self.user_name,
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
                          server_name=self.server_name,
                          user_name=self.user_name,
                          pid=self.pid,
                          rerun_id=self.initiator_id,
                          rerun_seqno=self.seqno,
                          recycle_ind=self.recycle_ind,
                          trigger_id=self.trigger_id,
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
                          error_list=self.text_error,
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

    def _shutdown(self, signum=None, frame=None):
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
        schedule = {'name': result.job_name,
                    'desc': result.job_description,
                    'debug': True if result.debug == 'Y' else False,
                    'alarm': True if result.alarm == 'Y' else False,
                    'email_list': result.email_list}
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
                            required=False, help='run timestamp')
        parser.add_argument('--date', type=dt.datetime.fromisoformat,
                            required=False, help='run date')
        parser.add_argument('--record', type=int,
                            required=False, help='run process ID')
        parser.add_argument('--trigger', type=int,
                            required=False, help='trigger process ID')
        parser.add_argument('--recycle', action='store_true',
                            required=False, help='recycle this job')
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

    def _parse_email_list(self, email_list):
        email_list = email_list or []
        if isinstance(email_list, (str, list)) is True:
            if isinstance(email_list, str) is True:
                if ',' in email_list:
                    email_list = email_list.replace(' ', '').split(',')
                else:
                    email_list = [email_list]
        else:
            email_list = []
        owners = logger.root.email.recipients or []
        email_list = [*email_list, *owners]
        return email_list

    def _set_signals(self):
        # Configure signal triggers.
        logger.debug('Setting signal triggers...')
        signal.signal(signal.SIGINT, self._shutdown)
        logger.debug(f'SIGINT trigger set for current PID[{self.pid}]')
        signal.signal(signal.SIGTERM, self._shutdown)
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
                        path = os.path.abspath(f'{self.path}/../{id}/job.py')
                        args += f' run -a --tag {tag} --trigger {trigger_id}'
                        proc = to_process(exe, path, args)
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

    def __init__(self, *nodes, name=None, date=None, error_limit=1,
                 logging=None):
        self.name = name or self.__class__.__name__
        self.date = date
        self.error_limit = error_limit

        self.task = Task()
        self.steps = {}
        self.nodes = {}
        self.threads = []

        self.job = Job.get() if Job.exists() else None
        if self.job:
            logger.debug(f'Using {self.job} configuration in {self}...')
            if self.job.name and not name:
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
        if self.job:
            return self.__class__.__name__
        else:
            return f'Pipeline[{self.name}]'

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
        return self.task.launch()

    def recycle(self):
        """Recycle pipeline."""
        return self.task.recycle()

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
        """Get process job."""
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

    def error_handler(self):
        """Process last error."""
        exc_info = sys.exc_info()
        if exc_info:
            self.errors.add(exc_info)
            if isinstance(self, Task):
                if self.job:
                    self.job.errors.add(exc_info)
            if isinstance(self, Step):
                if self.pipeline:
                    self.pipeline.task.errors.add(exc_info)
                    if self.job:
                        self.job.errors.add(exc_info)

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
        self._records_processed = 0
        self._records_error = 0
        self._result_value = 0
        self._result_long = 0
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
    def target(self):
        """Get target action of this task."""
        if self.job and self.job.target == 'recycle':
            return self.recycle
        else:
            return self.run

    @property
    def history(self):
        """Get task history."""
        if self.job and self.pipeline.date:
            conn = db.connect()
            th = db.tables.task_history
            select = th.select().where(
                sa.and_(
                    th.c.job_id == self.job.id,
                    th.c.task_name == self.name,
                    th.c.task_date == sql_preparer(self.pipeline.date, db)
                )
            )
            result = conn.execute(select)
            dataset = [dict(record) for record in result]
            return dataset

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
    def records_processed(self):
        """Get number of records processed."""
        return self._records_processed

    @records_written.setter
    def records_processed(self, value):
        if isinstance(value, int):
            self._records_processed += value
            self.logger.table(records_processed=self.records_processed)
        pass

    @property
    def records_error(self):
        """Get number of records with error."""
        return self._records_error

    @records_error.setter
    def records_error(self, value):
        if isinstance(value, int):
            self._records_error += value
            self.logger.table(records_error=self.records_error)
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

    def setup(self, pipeline):
        """Configure the task for the given pipeline."""
        self.pipeline = pipeline
        self.logger = self.pipeline.logging.task.setup()
        logger.debug(f'Logger {self.logger.name} is used in {self} logging')
        logger.debug(f'{self} configured in {pipeline}')
        pass

    def launch(self):
        """Launch task according to target."""
        return self.target()

    def run(self):
        """Run task from the very first pipeline steps."""
        self._prepare()
        self._start()
        try:
            self._run()
        except Exception:
            self._fail()
        self._end()
        return self.result()

    def recycle(self):
        """Recycle task."""
        logger.info(f'{self.pipeline} configured as a recycle run')
        logger.debug(f'{self} recycling...')
        self.revoke()
        logger.debug(f'{self} recycled...')
        return self.run()

    def revoke(self):
        """Revoke task."""
        if self.job and self.pipeline.date:
            conn = db.connect()
            th = db.tables.task_history
            for record in self.history:
                task_id = record['id']
                process_id = record['run_id']
                status = record['status']
                if status == 'D':
                    for step in self.pipeline.walk():
                        if step.last.recyclable:
                            if step.last.key_field.associated(self.job):
                                if process_id:
                                    step.last.recycle(process_id)
                                logger.debug(f'{step.last} recycled '
                                             f'by {process_id=}')
                            elif step.last.key_field.associated(self):
                                step.last.recycle(task_id)
                                logger.debug(f'{step.last} recycled '
                                             f'by {task_id=}')
                    update = th.update().values(status='C').\
                                where(th.c.id == task_id)
                    conn.execute(update)
                    logger.debug(f'{self} recycled by {task_id=}')

    def prepare(self):
        """Make all necessary task preparations."""
        pass

    def start(self):
        """Start task."""
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
        logger.info(f'Starting {self} from {self.pipeline}...')
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

    def _run(self):
        logger.info(f'{self} running')
        self.status = 'R'
        for step in self.pipeline.roots:
            step.launch()
        return self._wait()

    def _wait(self):
        """Wait until all pipeline threads are finished."""
        for thread in self.pipeline.threads:
            logger.debug(f'Waiting for {thread.name} in {self} to finish...')
            thread.join()
            logger.debug(f'{thread.name} in {self} finished')
        pass

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
        self._records_processed = 0
        self._records_error = 0
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
    def branch(self):
        """Get branch with nodes of this step."""
        if self.a is not None and self.b is None and self.c is None:
            return [self.a]
        elif self.a is not None and self.b is not None and self.c is None:
            return [self.a, self.b]
        elif self.a is not None and self.b is not None and self.c is not None:
            return [self.a, self.b, self.c]
        else:
            return None

    @property
    def first(self):
        """Get first node in the step."""
        return self.a

    @property
    def last(self):
        """Get last node in the step."""
        if self.b is None and self.c is None:
            return self.a
        elif self.c is None:
            return self.b
        else:
            return self.c

    @property
    def figure(self):
        """Get string representing step branch."""
        branch = [f'{node.name}' for node in self.branch]
        if branch and self.type == 'EX':
            return f'-->{branch[0]}-->'
        elif branch and self.type in ['EL', 'ETL']:
            return '-->'.join(branch)
        else:
            return None

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
    def target(self):
        """Get target action of this step."""
        if self.job and self.job.target == 'recycle':
            return self.recycle
        else:
            return self.run

    @property
    def history(self):
        """Get this step history."""
        if self.job and self.pipeline.date:
            conn = db.connect()
            sh = db.tables.step_history
            select = sh.select().where(
                sa.and_(
                    sh.c.job_id == self.job.id,
                    sh.c.step_name == self.name,
                    sh.c.step_date == sql_preparer(self.pipeline.date, db),
                    sh.c.step_a == (self.a.name if self.a else None),
                    sh.c.step_b == (self.b.name if self.b else None),
                    sh.c.step_c == (self.c.name if self.c else None)
                )
            )
            result = conn.execute(select)
            dataset = [dict(record) for record in result]
            return dataset

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
    def records_processed(self):
        """Get number of records processed."""
        return self._records_processed

    @records_written.setter
    def records_processed(self, value):
        if isinstance(value, int):
            self._records_processed += value
            self.logger.table(records_processed=self.records_processed)
        pass

    @property
    def records_error(self):
        """Get number of records with error."""
        return self._records_error

    @records_error.setter
    def records_error(self, value):
        print('Hello there!')
        if isinstance(value, int):
            self._records_error += value
            self.task.records_error = value
            self.logger.table(records_error=self.records_error)
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
        logger.debug(f'{self} {self.branch} configured in {pipeline}')
        pass

    def launch(self):
        """Launch this step in a separate thread."""
        logger.debug(f'Creating thread for {self} in {self.task}...')
        name = f'Step-{self.seqno}'
        self.thread = th.Thread(name=name, target=self.target, daemon=True)
        logger.debug(f'Created {self.thread.name} for {self} in {self.task}')
        self.pipeline.threads.append(self.thread)
        logger.debug(f'Starting {self.thread.name}...')
        return self.thread.start()

    def resume(self):
        """Proceed the process in the threads of the following steps."""
        if self.status == 'D':
            for step in self.last.joins:
                step.launch()
        pass

    def run(self):
        """Run this step."""
        self._start()
        try:
            self._run()
        except Exception:
            self._fail()
        self._end()
        return self.resume()

    def recycle(self):
        """Recycle this step."""
        try:
            logger.debug(f'{self} recycling...')
            self.revoke()
            logger.debug(f'{self} recycled')
            return self.run()
        except Exception:
            logger.error()

    def revoke(self):
        """Revoke this step."""
        if self.job and self.pipeline.date:
            conn = db.connect()
            sh = db.tables.step_history
            for record in self.history:
                step_id = record['id']
                status = record['status']
                if status == 'D':
                    if self.last.recyclable:
                        if self.last.key_field.associated(self):
                            self.last.recycle(step_id)
                            logger.debug(f'{self.last} recycled '
                                         f'by {step_id=}')
                    update = sh.update().values(status='C').\
                                where(sh.c.id == step_id)
                    conn.execute(update)
                    logger.debug(f'{self} recycled by {step_id=}')

    def _start(self):
        logger.info(f'Starting {self} from {self.pipeline}...')
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

    def _run(self):
        logger.info(f'{self} running')
        logger.info(f'{self} is {self.type} operation: {self.branch}')
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
