"""serialize/deserialize values between python and postgres"""
import datetime
import decimal
import random

import aiodb.connector.postgres.constants as constants
import aiodb.connector.serializer as shared


def to_boolean(value):
    """cast bool"""
    if value == 't':
        return True
    return False


def to_timestamp(value):
    """cast timestamp"""
    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')


def to_time(value):
    """cast time"""
    return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()


def from_postgres(data_type_id):
    """cast value from postgres to python"""
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
        constants.TYPE_DATE: shared.to_date,
    }.get(data_type_id, lambda x: x)


def quote(val):
    """quote val"""
    val = str(val)
    token = ''
    while True:
        delim = f'${token}$'
        if delim not in val:
            break
        token += random.choice('0123456789abcdef')
    return f"{delim}{val}{delim}"


def from_bytes(val):
    """escape a python byte array"""
    return quote(val.decode('ascii', 'surrogateescape'))


def from_set(val):
    """escape a python set"""
    return quote(','.join([str(item) for item in val]))


def to_postgres(val):
    """prepare val for use in a sql statement"""
    if val is None:
        return 'NULL'

    return {
        int: quote,
        bool: quote(shared.from_bool),
        str: quote,
        float: quote(shared.from_float),
        datetime.date: shared.from_date,
        bytes: from_bytes,
        datetime.timedelta: shared.from_timedelta,
        datetime.datetime: shared.from_datetime,
        datetime.time: shared.from_time,
        # time.struct_time: escape_struct_time,
        decimal.Decimal: quote,
        set: from_set,
    }[type(val)](val)
