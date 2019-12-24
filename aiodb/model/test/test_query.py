import pytest
from aiodb import Model, Field
from aiodb.model.query import QueryTable
from aiodb.model.query import _find_foreign_key_reference
from aiodb.model.query import _find_primary_key_reference
from aiodb.model.query import _pair


class A(Model):
    id = Field(is_primary=True)


class B(Model):
    id = Field(is_primary=True)
    a_id = Field(foreign='test_query.A')
    c_id = Field(foreign='test_query.C')


class C(Model):
    id = Field(is_primary=True)
    a_id = Field(foreign='test_query.A')


@pytest.mark.parametrize(
    'table,tables,is_none,rtable,rfield', (
        (B, (A,), False, A, 'a_id'),
        (A, (B,), True, 0, 0),
    ),
)
def test_find_foreign(table, tables, is_none, rtable, rfield):
    result = _find_foreign_key_reference(table, tables)
    if is_none:
        assert result is None
    else:
        table, field = result
        assert table == rtable
        assert field == rfield


def test_find_foreign_multiple():
    with pytest.raises(TypeError):
        _find_foreign_key_reference(B, (A, C))


def test_find_primary():
    table, field = _find_primary_key_reference(A, (B,))
    assert table == B
    assert field == 'a_id'

    assert _find_primary_key_reference(B, (A,)) is None


def test_find_primary_multiple():
    with pytest.raises(TypeError):
        _find_primary_key_reference(A, (B, C))


@pytest.mark.parametrize(
    't1,t2,a,c1,t,c2', (
        (A, B, None, 'a_id', 'a', 'id'),
        (B, A, None, 'id', 'b', 'a_id'),
        ((A, B), C, None, 'a_id', 'a', 'id'),
        ((A, C), B, 'a', 'a_id', 'a', 'id'),
        ((A, C), B, A, 'a_id', 'a', 'id'),
    )
)
def test_pair(t1, t2, a, c1, t, c2):
    if not isinstance(t1, tuple):
        t1 = (t1,)
    q = [QueryTable(tab) for tab in t1]
    column1, table2, column2 = _pair(t2, q, a)
    assert column1 == c1
    assert t == table2
    assert column2 == c2
