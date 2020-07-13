"""serialize/deserialize values between python and mysql"""
import datetime
import decimal
import json

import aiodb.connector.serializer as shared
from aiodb.connector.mysql.bit import Bit
from aiodb.connector.mysql.constants import FIELD_TYPE


def bytes_to_int(value):
    """cast byte value to int"""
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
    FIELD_TYPE.TIMESTAMP: shared.to_datetime,
    FIELD_TYPE.DATE: shared.to_date,
    FIELD_TYPE.TIME: shared.to_time,
    FIELD_TYPE.DATETIME: shared.to_datetime,
    FIELD_TYPE.YEAR: int,
    FIELD_TYPE.JSON: json.loads,
    FIELD_TYPE.NEWDECIMAL: decimal.Decimal,
    FIELD_TYPE.BIT: bytes_to_int,
}


def quote(val):
    """quote val"""
    return f"'{val}'"


def from_float(val):
    """escape a python float"""
    return quote(shared.from_float(val))


def from_bool(val):
    """escape a python bool"""
    return quote(shared.from_bool(val))


def from_string(val):
    """escape a python string"""
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
    """escape a python byte array"""
    return from_string(val.decode('ascii', 'surrogateescape'))


def from_bit(val):
    """escape a aiodb.connector.mysql.Bit"""
    return f"b'{val.as_binary()}'"


def from_set(val):
    """escape a python set"""
    return from_string(','.join([str(item) for item in val]))


def to_mysql(val):
    """prepare val for use in sql statement"""
    if val is None:
        return 'NULL'

    return {
        bool: from_bool,
        int: quote,
        float: from_float,
        str: from_string,
        bytes: from_bytes,
        datetime.date: shared.from_date,
        datetime.datetime: shared.from_datetime,
        datetime.timedelta: shared.from_timedelta,
        datetime.time: shared.from_time,
        # time.struct_time: escape_struct_time,
        decimal.Decimal: quote,
        set: from_set,
        Bit: from_bit
    }[type(val)](val)
