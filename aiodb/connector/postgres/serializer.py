import datetime
import decimal
import random


def quote(val):
    val = str(val)
    token = ''
    while True:
        delim = f'${token}$'
        if delim not in val:
            break
        token += random.choice('0123456789abcdef')
    return f"{delim}{val}{delim}"


def from_bool(val):
    return quote(int(val))


def from_float(val):
    return quote('%.15g' % val)


def from_string(val):
    escape = val.translate(
        str.maketrans({
            "'": "''",
            '\\': '\\\\',
            '\0': '\\0',
            '\b': '\\b',
            '\n': '\\n',
            '\r': '\\r',
            '\t': '\\t',
            # ctrl z: windows end-of-file; escaped z deprecated in python
            '\x1a': '\\x1a',
        }))
    return f"'{escape}'"


def from_bytes(val):
    return from_string(val.decode('ascii', 'surrogateescape'))


def from_date(val):
    return val.strftime("'%Y-%m-%d'")


def from_datetime(val):
    return val.strftime("'%Y-%m-%d %H:%M:%S.%f'")


def _time(hour, minute, second, microsecond):
    if microsecond:
        return f"'{hour:02d}:{minute:02d}:{second:02d}.{microsecond:06d}'"
    return f"'{hour:02d}:{minute:02d}:{second:02d}'"


def from_time(val):
    return _time(val.hour, val.minute, val.second, val.microsecond)


def from_timedelta(val):
    sec = int(val.total_seconds())
    h = sec // 3600
    sec = sec % 3600
    m = sec // 60
    s = sec % 60
    ms = val.microseconds
    return _time(h, m, s, ms)


def from_set(val):
    return from_string(','.join([str(item) for item in val]))


def to_postgres(val):
    if val is None:
        return 'NULL'

    return {
        bool: from_bool,
        int: quote,
        float: from_float,
        str: from_string,
        bytes: from_bytes,
        datetime.date: from_date,
        datetime.datetime: from_datetime,
        datetime.timedelta: from_timedelta,
        datetime.time: from_time,
        # time.struct_time: escape_struct_time,
        decimal.Decimal: quote,
        set: from_set,
    }[type(val)](val)
