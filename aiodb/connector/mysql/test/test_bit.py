import pytest

from aiodb.connector.mysql.bit import Bit


@pytest.mark.parametrize(
    'length, input, expected', (
        (10, None, ValueError),
        (10, 1, 1),
        (10, '123', TypeError),
        (10, '0', 0),
        (10, '1', 1),
        (10, '010', 2),
        (10, '1010', 10),
        (10, '01000000000', ValueError),
    ),
)
def test_from_date(length, input, expected):
    bit = Bit(length)
    if expected in (ValueError, TypeError):
        with pytest.raises(expected):
            bit(input)
    else:
        assert bit(input).value == expected


def test_as_binary():
    bit = Bit(5)(10)
    assert bit.as_binary() == '1010'
