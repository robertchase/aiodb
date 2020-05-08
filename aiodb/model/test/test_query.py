import pytest
from aiodb import Model, Field
from aiodb.model.query import QueryTable
from aiodb.model.query import _find_foreign_key_reference
from aiodb.model.query import _find_primary_key_reference
from aiodb.model.query import _pair


class A(Model):
    __TABLENAME__ = 'yikes'
    id = Field(is_primary=True)


class B(Model):
    __TABLENAME__ = 'yeah'
    id = Field(is_primary=True)
    a_id = Field(foreign='test_query.A')
    c_id = Field(foreign='test_query.C')


class C(Model):
    id = Field(is_primary=True)
    a_id = Field(foreign='test_query.A')


class D(Model):
    __TABLENAME__ = 'd'
    a = Field()
    b = Field(expression='NOW()')
    c = Field(expression='FN({Q}z{Q})')


def test_expression():
    stmt = D.query._build(False, None, None, None, "'")
    result = \
        "SELECT 'd'.'a' AS 0_a, NOW() AS 0_b, FN('z') AS 0_c FROM 'd' AS 'd'"
    assert stmt == result


def test_table_name():
    query = A.query.where('{TABLE.A}.id=10')
    stmt = query._build(False, None, None, None, "'")
    expect = (
        "SELECT 'a'.'id' AS 0_id FROM 'yikes' AS 'a'"
        " WHERE 'a'.id=10"
    )
    assert stmt == expect


def test_table_names():
    query = A.query.join(B, alias='FOO').where('{TABLE.A}.id={TABLE.FOO}.a')
    stmt = query._build(False, None, None, None, "'")
    expect = (
        "SELECT 'a'.'id' AS 0_id, 'FOO'.'a_id' AS 1_a_id,"
        " 'FOO'.'c_id' AS 1_c_id, 'FOO'.'id' AS 1_id"
        " FROM 'yikes' AS 'a' JOIN 'yeah' AS 'FOO'"
        " ON 'FOO'.'a_id' = 'a'.'id' WHERE 'a'.id='FOO'.a"
    )
    assert stmt == expect


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
