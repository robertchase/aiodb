"""validate type operation"""
import datetime
import pytest

from aiodb.model.types import Char, Enum
from aiodb.model.types import String, Integer, Boolean, Date, Datetime, Time


@pytest.mark.parametrize(
    'length,value,expected,strict', (
        (5, 1, '1    ', False),
        (2, '1', '1 ', False),
        (2, '123', '12', False),
        (2, '123', ValueError, True),
        (4, '123', '123 ', True),
        (10, None, ValueError, False),
        (5, 123.4, '123.4', False),
    ),
)
def test_char_parse(length, value, expected, strict):
    """validate char parsing"""
    test = Char(length, strict)
    if expected == ValueError:
        with pytest.raises(expected):
            test.parse(value)
    else:
        assert test.parse(value) == expected


@pytest.mark.parametrize(
    'allowed,value,expected', (
        (('FRED', 'WILMA', 'FIDO'), 'FRED', None),
        (('FRED', 'WILMA', 'FIDO'), 'WILMA', None),
        (('FRED', 'WILMA', 'FIDO'), 'WHAT', ValueError),
        (('FRED', 'WILMA', 'FIDO'), 'fred', ValueError),
    ),
)
def test_enum_parse(allowed, value, expected):
    """validate enum parsing"""
    test = Enum(*allowed)
    if expected == ValueError:
        with pytest.raises(expected):
            test.parse(value)
    else:
        assert test.parse(value) == expected or value


@pytest.mark.parametrize(
    'value,expected', (
        (1, '1'),
        ('1', '1'),
        (123.4, '123.4'),
        (String, "<class 'aiodb.model.types.String'>"),
        (None, ValueError),
    ),
)
def test_string_parse(value, expected):
    """validate string parsing"""
    if expected == ValueError:
        with pytest.raises(expected):
            String.parse(value)
    else:
        assert String.parse(value) == expected


@pytest.mark.parametrize(
    'value,expected', (
        (1, 1),
        ('1', 1),
        (1.0, 1),
        (0, 0),
        (-1, -1),
        (123456789123456789123456789, ValueError),
        ('1.0', ValueError),
        ('1.1', ValueError),
        ('dude', ValueError),
        (None, ValueError),
    ),
)
def test_integer_parse(value, expected):
    """validate integer parsing"""
    if expected == ValueError:
        with pytest.raises(expected):
            Integer.parse(value)
    else:
        assert Integer.parse(value) == expected


@pytest.mark.parametrize(
    'value,expected', (
        (True, 1),
        (False, 0),
        (1, 1),
        (0, 0),
        (-1, ValueError),
        ('True', 1),
        ('False', 0),
        ('TrUe', 1),
        ('FaLsE', 0),
        ('t', 1),
        ('f', 0),
        ('T', 1),
        ('F', 0),
        (None, ValueError),
    ),
)
def test_boolean_parse(value, expected):
    """validate boolean parsing"""
    if expected == ValueError:
        with pytest.raises(expected):
            Boolean.parse(value)
    else:
        assert Boolean.parse(value) == expected


DATE_STRING = '2020-01-02'
DATE_DATE = datetime.datetime(2020, 1, 2).date()
DATE_DATETIME = datetime.datetime(2020, 1, 2, 12)


@pytest.mark.parametrize(
    'value,expected', (
        (None, ValueError),
        (DATE_STRING, DATE_DATE),
        ('garbage', ValueError),
        (DATE_DATE, DATE_DATE),
        (DATE_DATETIME, DATE_DATE),
        (1, TypeError),
    ),
)
def test_date_parse(value, expected):
    """validate date parsing"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Date.parse(value)
    else:
        assert Date.parse(value) == expected


DATETIME_STRING = '2020-01-02 11:12:13'
DATETIME_DATE = datetime.datetime(2020, 1, 2).date()
DATETIME_DATETIME = datetime.datetime(2020, 1, 2, 11, 12, 13)
DATETIME_US_STRING = '2020-01-02 11:12:13.123456'
DATETIME_US_DATETIME = datetime.datetime(2020, 1, 2, 11, 12, 13, 123456)


@pytest.mark.parametrize(
    'value,expected', (
        (None, ValueError),
        ('garbage', ValueError),
        (1, TypeError),
        (DATETIME_STRING, DATETIME_DATETIME),
        (DATETIME_DATE, datetime.datetime(
            DATETIME_DATE.year, DATETIME_DATE.month, DATETIME_DATE.day
        )),
        (DATETIME_DATETIME, DATETIME_DATETIME),
        (DATETIME_US_STRING, DATETIME_US_DATETIME),
    ),
)
def test_datetime_parse(value, expected):
    """valudate datetime parsing"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Datetime.parse(value)
    else:
        assert Datetime.parse(value) == expected


TIME_LARGE_STRING = '111:12:13'
TIME_LARGE_TIME = datetime.timedelta(hours=111, minutes=12, seconds=13)
TIME_STRING = '11:12:13'
TIME_TIME = datetime.time(11, 12, 13)
TIME_US_STRING = '11:12:13.123456'
TIME_US_TIME = datetime.time(11, 12, 13, 123456)
TIME_US_STRING_2 = '11:12:13.123'
TIME_US_TIME_2 = datetime.time(11, 12, 13, 123000)


@pytest.mark.parametrize(
    'value,expected', (
        (None, ValueError),
        ('garbage', ValueError),
        (1, ValueError),
        (TIME_STRING, TIME_TIME),
        (TIME_US_STRING, TIME_US_TIME),
        (TIME_US_STRING_2, TIME_US_TIME_2),
        (TIME_LARGE_STRING, TIME_LARGE_TIME),
    ),
)
def test_time_parse(value, expected):
    """validate time parsing"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Time.parse(value)
    else:
        assert Time.parse(value) == expected
