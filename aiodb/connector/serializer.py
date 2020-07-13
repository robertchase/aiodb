"""shared serializer/deserializer logic"""
import datetime

from aiodb.model.types import to_datetime, to_time


def to_date(value):
    """parse date value"""
    return datetime.datetime.strptime(value, '%Y-%m-%d').date()


def from_bool(val):
    """escape a python bool"""
    return int(val)


def from_float(val):
    """escape a python float"""
    return '%.15g' % val


def from_date(val):
    """escape a python datetime.date"""
    return val.strftime("'%Y-%m-%d'")


def from_datetime(val):
    """escape a python datetime.datetime"""
    return val.strftime("'%Y-%m-%d %H:%M:%S.%f'")


def _time(hour, minute, second, microsecond):
    if microsecond:
        return f"'{hour:02d}:{minute:02d}:{second:02d}.{microsecond:06d}'"
    return f"'{hour:02d}:{minute:02d}:{second:02d}'"


def from_time(val):
    """escape a python datetime.time"""
    return _time(val.hour, val.minute, val.second, val.microsecond)


def from_timedelta(val):
    """escape a python datetime.timedelta"""
    sec = int(val.total_seconds())
    hour = sec // 3600
    sec = sec % 3600
    mns = sec // 60
    sec = sec % 60
    msec = val.microseconds
    return _time(hour, mns, sec, msec)
