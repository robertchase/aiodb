import datetime
import pytest

import aiodb.connector.mysql.converters as converters


@pytest.mark.parametrize(
    'input,expected', (
        # normal
        ('a', "'a'"),
        # quotes
        ("'a", "'''a'"),
        ('"a', "'\"a'"),
        # backslash
        (r'\a', r"'\\a'"),
        # null
        ('a\0b', r"'a\0b'"),
        # backspace
        ('a\bb', r"'a\bb'"),
        # newline
        ('a\bb', r"'a\bb'"),
        # carriage return
        ('a\rb', r"'a\rb'"),
        # tab
        ('a\tb', r"'a\tb'"),
        # ctrl-z
        (chr(26), r"'\x1a'"),
        # combined
        ((' "abc"\r\n, \t\x00123'), ("' \"abc\"\\r\\n, \\t\\0123'")),
    ),
)
def test_string(input, expected):
    assert converters.from_string(input) == expected


date_string = '2020-01-02'
date_date = datetime.datetime(2020, 1, 2).date()
date_datetime = datetime.datetime(2020, 1, 2, 12)


@pytest.mark.parametrize(
    'input,expected', (
        (date_date, f"'{date_string}'"),
        (date_datetime, f"'{date_string}'"),
    ),
)
def test_from_date(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            converters.from_date(input)
    else:
        assert converters.from_date(input) == expected


datetime_string = '2020-01-02 11:12:13.000000'
datetime_date = datetime.datetime(2020, 1, 2).date()
datetime_datetime = datetime.datetime(2020, 1, 2, 11, 12, 13)


@pytest.mark.parametrize(
    'input,expected', (
        (datetime_date, f"'{date_string} 00:00:00.000000'"),
        (datetime_datetime, f"'{datetime_string}'"),
    ),
)
def test_from_datetime(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            converters.from_datetime(input)
    else:
        assert converters.from_datetime(input) == expected


time_string = '01:01:01'
time_time = datetime.time(1, 1, 1)
time_delta = datetime.timedelta(seconds=3600 + 60 + 1)
time_mu_string = '01:01:01.000123'
time_mu_time = datetime.time(1, 1, 1, 123)
time_mu_delta = datetime.timedelta(seconds=3600 + 60 + 1, microseconds=123)


@pytest.mark.parametrize(
    'input,expected', (
        (time_delta, f"'{time_string}'"),
        (time_mu_delta, f"'{time_mu_string}'"),
    ),
)
def test_from_timedelta(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            converters.from_timedelta(input)
    else:
        assert converters.from_timedelta(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        (time_time, f"'{time_string}'"),
        (time_mu_time, f"'{time_mu_string}'"),
    ),
)
def test_from_time(input, expected):
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            converters.from_time(input)
    else:
        assert converters.from_time(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        (set(('a', 'b')), ("'a,b'", "'b,a'")),
        (set((1, 'b')), ("'1,b'", "'b,1'")),
        (set((1, 2)), ("'1,2'", "'2,1'")),
    ),
)
def test_from_set(input, expected):
    assert converters.from_set(input) in expected
