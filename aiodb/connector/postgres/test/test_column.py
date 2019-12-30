import datetime
import decimal
import pytest

import aiodb.connector.postgres.column as column


@pytest.mark.parametrize(
    'input,expected', (
        ('t', True),
        ('T', False),
        (True, False),
        (None, False),
    ),
)
def test_boolean(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_BOOLEAN
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        (1, 1),
        (0, 0),
        (100.0, 100),
    ),
)
def test_int(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_INT8
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('asdf', 'asdf'),
        (b'asdf', b'asdf'),
        (None, None),
    ),
)
def test_text(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_TEXT
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('1.2', decimal.Decimal('1.2')),
        (1, decimal.Decimal('1')),
        (1.5, decimal.Decimal('1.5')),
        (-1.5, decimal.Decimal('-1.5')),
    ),
)
def test_numeric(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_NUMERIC
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('2020-01-02 12:13:14.123456',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456)),
    ),
)
def test_timestamp(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_TIMESTAMP
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('12:13:14.123456',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456).time()),
    ),
)
def test_time(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_TIME
    assert d.convert(input) == expected


@pytest.mark.parametrize(
    'input,expected', (
        ('2020-01-02',
         datetime.datetime(2020, 1, 2, 12, 13, 14, 123456).date()),
    ),
)
def test_date(input, expected):
    d = column.Descriptor()
    d.data_type_id = column.TYPE_DATE
    assert d.convert(input) == expected
