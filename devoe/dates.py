import calendar
import datetime as dt

def verify_timezone(func):
    def wrapper(today):
        now = func(today)
        if today.timezone is not None:
            return now.astimezone(tz=today.timezone)
        else:
            return now
    return wrapper

class Today():
    def __init__(self, now=None):
        self._now = now or dt.datetime.now()
        self.timezone = None
        pass

    @property
    @verify_timezone
    def now(self):
        return self._now

    @property
    @verify_timezone
    def start(self):
        return self._now.replace(hour=0, minute=0, second=0)

    @property
    @verify_timezone
    def end(self):
        return self._now.replace(hour=23, minute=59, second=59)

    @property
    def hour(self):
        return Hour(self._now)

    @property
    def yesterday(self):
        return Yesterday(self._now)

    @property
    def tomorrow(self):
        return Tomorrow(self._now)

    @property
    def month(self):
        return Month(self._now)

    @property
    def year(self):
        return Year(self._now)

    @property
    def utc(self):
        self.timezone = dt.timezone.utc
        return self

class Hour(Today):
    @property
    def before(self):
        return Hour(self._now - dt.timedelta(hours=1))

    @property
    @verify_timezone
    def start(self):
        return self._now.replace(minute=0, second=0)

    @property
    @verify_timezone
    def end(self):
        return self._now.replace(minute=59, second=59)

class Month(Today):
    @property
    def before(self):
        return Month(self._now.replace(day=1) - dt.timedelta(days=1))

    @property
    @verify_timezone
    def start(self):
        return self._now.replace(day=1, hour=0, minute=0, second=0)

    @property
    @verify_timezone
    def end(self):
        days = calendar.monthrange(self._now.year, self._now.month)[1]
        return self._now.replace(day=days, hour=23, minute=59, second=59)

class Year(Today):
    @property
    def before(self):
        return Year(self._now - dt.timedelta(days=365))

    @property
    @verify_timezone
    def start(self):
        return self._now.replace(month=1, day=1, hour=0, minute=0, second=0)

    @property
    @verify_timezone
    def end(self):
        return self._now.replace(month=12, day=31, hour=23, minute=59,
                                 second=59)

class Yesterday(Today):
    def __init__(self, now):
        now = now - dt.timedelta(days=1)
        super().__init__(now)
        pass

class Tomorrow(Yesterday):
    def __init__(self, now):
        now = now + dt.timedelta(days=1)
        super().__init__(now)
        pass
