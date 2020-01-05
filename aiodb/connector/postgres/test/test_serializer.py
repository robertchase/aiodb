import datetime
import random

import pytest

import aiodb.connector.postgres.serializer as serializer


@pytest.mark.parametrize(
    'input,expected', (
        ('a', '$$a$$'),
        ('a$$a$$a', '$4$a$$a$$a$4$'),
        ('a$$4$$a', '$4e$a$$4$$a$4e$'),
    ),
)
def test_quote(input, expected):
    random.seed(100)
    assert serializer.quote(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('t', True),
        ('T', False),
        (True, False),
        (None, False),
    ),
)
def test_to_boolean(input, expected):
    assert serializer.to_boolean(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('2020-01-02 12:13:14.123456',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456)),
    ),
)
def test_to_timestamp(input, expected):
    assert serializer.to_timestamp(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('12:13:14.123456',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456).time()),
    ),
)
def test_to_time(input, expected):
    assert serializer.to_time(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('2020-01-02',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456).date()),
    ),
)
def test_to_date(input, expected):
    assert serializer.to_date(input) == expected
