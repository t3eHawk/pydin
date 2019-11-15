import os
import datetime as dt

who = lambda: os.getlogin()
when = lambda: dt.datetime.now()
str_to_dttm = lambda string: dt.datetime.fromisoformat(string)
dttm_to_utc = lambda datetime: datetime.astimezone(tz=dt.timezone.utc)
