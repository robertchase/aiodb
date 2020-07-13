"""test serializer operation"""
import datetime
import pytest

import aiodb.connector.serializer as serializer


DATE_STRING = '2020-01-02'
DATE_DATE = datetime.datetime(2020, 1, 2).date()
DATE_DATETIME = datetime.datetime(2020, 1, 2, 12)


@pytest.mark.parametrize(
    'value,expected', (
        (DATE_DATE, f"'{DATE_STRING}'"),
        (DATE_DATETIME, f"'{DATE_STRING}'"),
    ),
)
def test_from_date(value, expected):
    """test from_date"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            serializer.from_date(value)
    else:
        assert serializer.from_date(value) == expected


DATETIME_STRING = '2020-01-02 11:12:13.000000'
DATETIME_DATE = datetime.datetime(2020, 1, 2).date()
DATETIME_DATETIME = datetime.datetime(2020, 1, 2, 11, 12, 13)


@pytest.mark.parametrize(
    'value,expected', (
        (DATETIME_DATE, f"'{DATE_STRING} 00:00:00.000000'"),
        (DATETIME_DATETIME, f"'{DATETIME_STRING}'"),
    ),
)
def test_from_datetime(value, expected):
    """test from_datetime"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            serializer.from_datetime(value)
    else:
        assert serializer.from_datetime(value) == expected


TIME_STRING = '01:01:01'
TIME_TIME = datetime.time(1, 1, 1)
TIME_DELTA = datetime.timedelta(seconds=3600 + 60 + 1)
TIME_MU_STRING = '01:01:01.000123'
TIME_MU_TIME = datetime.time(1, 1, 1, 123)
TIME_MU_DELTA = datetime.timedelta(seconds=3600 + 60 + 1, microseconds=123)


@pytest.mark.parametrize(
    'value,expected', (
        (TIME_DELTA, f"'{TIME_STRING}'"),
        (TIME_MU_DELTA, f"'{TIME_MU_STRING}'"),
    ),
)
def test_from_timedelta(value, expected):
    """test from_timedelta"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            serializer.from_timedelta(value)
    else:
        assert serializer.from_timedelta(value) == expected


@pytest.mark.parametrize(
    'value,expected', (
        (TIME_TIME, f"'{TIME_STRING}'"),
        (TIME_MU_TIME, f"'{TIME_MU_STRING}'"),
    ),
)
def test_from_time(value, expected):
    """test from_time"""
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            serializer.from_time(value)
    else:
        assert serializer.from_time(value) == expected
