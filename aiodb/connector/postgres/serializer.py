import datetime
import decimal
import random

import aiodb.connector.postgres.constants as constants


def to_boolean(value):
    return True if value == 't' else False


def to_timestamp(value):
    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')


def to_time(value):
    return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()


def to_date(value):
    return datetime.datetime.strptime(value, '%Y-%m-%d').date()


def from_postgres(data_type_id):
    return {
        constants.TYPE_BOOLEAN: to_boolean,
        constants.TYPE_INT2: int,
        constants.TYPE_INT4: int,
        constants.TYPE_INT8: int,
        constants.TYPE_NUMERIC: decimal.Decimal,
        constants.TYPE_FLOAT4: float,
        constants.TYPE_FLOAT8: float,
        constants.TYPE_TIMESTAMP: to_timestamp,
        constants.TYPE_TIME: to_time,
        constants.TYPE_DATE: to_date,
    }.get(data_type_id, lambda x: x)


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


def from_bytes(val):
    return quote(val.decode('ascii', 'surrogateescape'))


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
    return quote(','.join([str(item) for item in val]))


def to_postgres(val):
    if val is None:
        return 'NULL'

    return {
        bool: from_bool,
        int: quote,
        float: from_float,
        str: quote,
        bytes: from_bytes,
        datetime.date: from_date,
        datetime.datetime: from_datetime,
        datetime.timedelta: from_timedelta,
        datetime.time: from_time,
        # time.struct_time: escape_struct_time,
        decimal.Decimal: quote,
        set: from_set,
    }[type(val)](val)
