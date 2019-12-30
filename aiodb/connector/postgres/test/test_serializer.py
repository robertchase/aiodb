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
