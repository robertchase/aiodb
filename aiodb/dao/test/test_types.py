import datetime
import pytest

from aiodb.dao.types import CHAR
from aiodb.dao.types import String, Integer, Boolean, Date, Datetime, Time


@pytest.mark.parametrize(
    'length,input,expected,strict', (
        (5, 1, '1    ', False),
        (2, '1', '1 ', False),
        (2, '123', '12', False),
        (2, '123', ValueError, True),
        (4, '123', '123 ', True),
        (10, None, ValueError, False),
        (5, 123.4, '123.4', False),
    ),
)
def test_char_parse(length, input, expected, strict):
    if expected == ValueError:
        with pytest.raises(expected):
            CHAR(length, strict).parse(input)
    else:
        assert CHAR(length, strict).parse(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        (1, '1'),
        ('1', '1'),
        (123.4, '123.4'),
        (String, "<class 'aiodb.dao.types.String'>"),
        (None, ValueError),
    ),
)
def test_string_parse(input, expected):
    if expected == ValueError:
        with pytest.raises(expected):
            String.parse(input)
    else:
        assert String.parse(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
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
def test_integer_parse(input, expected):
    if expected == ValueError:
        with pytest.raises(expected):
            Integer.parse(input)
    else:
        assert Integer.parse(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
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
def test_boolean_parse(input, expected):
    if expected == ValueError:
        with pytest.raises(expected):
            Boolean.parse(input)
    else:
        assert Boolean.parse(input) == expected


date_string = '2020-01-02'
date_date = datetime.datetime(2020, 1, 2).date()
date_datetime = datetime.datetime(2020, 1, 2, 12)


@pytest.mark.parametrize(
    'input,expected', (
        (None, ValueError),
        (date_string, date_date),
        ('garbage', ValueError),
        (date_date, date_date),
        (date_datetime, date_date),
        (1, TypeError),
    ),
)
def test_date_parse(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Date.parse(input)
    else:
        assert Date.parse(input) == expected


datetime_string = '2020-01-02 11:12:13'
datetime_date = datetime.datetime(2020, 1, 2).date()
datetime_datetime = datetime.datetime(2020, 1, 2, 11, 12, 13)
datetime_us_string = '2020-01-02 11:12:13.123456'
datetime_us_datetime = datetime.datetime(2020, 1, 2, 11, 12, 13, 123456)


@pytest.mark.parametrize(
    'input,expected', (
        (None, ValueError),
        ('garbage', ValueError),
        (1, TypeError),
        (datetime_string, datetime_datetime),
        (datetime_date, datetime.datetime(
            datetime_date.year, datetime_date.month, datetime_date.day
        )),
        (datetime_datetime, datetime_datetime),
        (datetime_us_string, datetime_us_datetime),
    ),
)
def test_datetime_parse(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Datetime.parse(input)
    else:
        assert Datetime.parse(input) == expected


time_large_string = '111:12:13'
time_large_time = datetime.timedelta(hours=111, minutes=12, seconds=13)
time_string = '11:12:13'
time_time = datetime.time(11, 12, 13)
time_us_string = '11:12:13.123456'
time_us_time = datetime.time(11, 12, 13, 123456)
time_us_string_2 = '11:12:13.123'
time_us_time_2 = datetime.time(11, 12, 13, 123000)


@pytest.mark.parametrize(
    'input,expected', (
        (None, ValueError),
        ('garbage', ValueError),
        (1, ValueError),
        (time_string, time_time),
        (time_us_string, time_us_time),
        (time_us_string_2, time_us_time_2),
        (time_large_string, time_large_time),
    ),
)
def test_time_parse(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            Time.parse(input)
    else:
        assert Time.parse(input) == expected
