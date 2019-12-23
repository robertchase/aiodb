import datetime
import decimal
import json

import aiodb.dao.types as types

from aiodb.connector.mysql.bit import Bit
from aiodb.connector.mysql.constants import FIELD_TYPE


def bytes_to_int(value):
    return int.from_bytes(value, 'big')


from_mysql = {
    FIELD_TYPE.DECIMAL: decimal.Decimal,
    FIELD_TYPE.TINY: int,
    FIELD_TYPE.SHORT: int,
    FIELD_TYPE.INT24: int,
    FIELD_TYPE.LONG: int,
    FIELD_TYPE.LONGLONG: int,
    FIELD_TYPE.FLOAT: float,
    FIELD_TYPE.DOUBLE: float,
    FIELD_TYPE.TIMESTAMP: types.Datetime.parse,
    FIELD_TYPE.DATE: types.Date.parse,
    FIELD_TYPE.TIME: types.Time.parse,
    FIELD_TYPE.DATETIME: types.Datetime.parse,
    FIELD_TYPE.YEAR: int,
    FIELD_TYPE.JSON: json.loads,
    FIELD_TYPE.NEWDECIMAL: decimal.Decimal,
    FIELD_TYPE.BIT: bytes_to_int,
}


def quote(val):
    return f"'{val}'"


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


def from_bit(val):
    return f"b'{val.as_binary()}'"


def from_set(val):
    return from_string(','.join([str(item) for item in val]))


def to_mysql(val):
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
        Bit: from_bit
    }[type(val)](val)
