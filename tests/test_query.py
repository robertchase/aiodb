"""test query operations"""
import pytest
from aiodb import Model, Field
from aiodb.model.query import QueryTable
from aiodb.model.query import _find_foreign_key_reference
from aiodb.model.query import _find_primary_key_reference
from aiodb.model.query import _pair


class A(Model):  # pylint: disable=invalid-name
    """test model"""
    TABLENAME = "yikes"
    id = Field(is_primary=True)


class B(Model):  # pylint: disable=invalid-name
    """test model"""
    TABLENAME = "yeah"
    id = Field(is_primary=True)
    a_id = Field(foreign='tests.test_query.A')
    c_id = Field(foreign='tests.test_query.C')


class C(Model):  # pylint: disable=invalid-name
    """test model"""
    id = Field(is_primary=True)
    a_id = Field(foreign='tests.test_query.A')


class D(Model):  # pylint: disable=invalid-name
    """test model"""
    TABLENAME = "d"
    a = Field()
    b = Field(expression='NOW()')
    c = Field(expression='FN({Q}z{Q})')


def test_expression():
    """verify simple SELECT expression"""
    stmt = D.query._prepare(  # pylint: disable=protected-access
        False, None, None, None, "'")
    result = \
        "SELECT 'd'.'a' AS 0_a, NOW() AS 0_b, FN('z') AS 0_c FROM 'd' AS 'd'"
    assert stmt == result


def test_table_name():
    """verify _build use of table name"""
    query = A.query.where('{TABLE.A}.id=10')
    stmt = query._prepare(  # pylint: disable=protected-access
        False, None, None, None, "'")
    expect = (
        "SELECT 'a'.'id' AS 0_id FROM 'yikes' AS 'a'"
        " WHERE 'a'.id=10"
    )
    assert stmt == expect


def test_table_names():
    """verify _build use of table names"""
    query = A.query.join(B, alias='FOO').where('{TABLE.A}.id={TABLE.FOO}.a')
    stmt = query._prepare(  # pylint: disable=protected-access
        False, None, None, None, "'")
    expect = (
        "SELECT 'a'.'id' AS 0_id,"
        " 'FOO'.'id' AS 1_id,"
        " 'FOO'.'a_id' AS 1_a_id,"
        " 'FOO'.'c_id' AS 1_c_id"
        " FROM 'yikes' AS 'a' JOIN 'yeah' AS 'FOO'"
        " ON 'FOO'.'a_id' = 'a'.'id' WHERE 'a'.id='FOO'.a"
    )
    assert stmt == expect


def test_duplicate():
    """verify duplicate table detection"""
    with pytest.raises(ValueError) as ex:
        A.query.join(C).join(C)
    assert ex.value.args[0] == "duplicate table 'c'"
    A.query.join(C).join(C, alias='CC')


@pytest.mark.parametrize(
    'table,tables,is_none,rtable,rfield', (
        (B, (A,), False, A, 'a_id'),
        (A, (B,), True, 0, 0),
    ),
)
def test_find_foreign(table, tables, is_none, rtable, rfield):
    """test join on foreign key"""
    result = _find_foreign_key_reference(table, tables)
    if is_none:
        assert result is None
    else:
        table, field = result
        assert table == rtable
        assert field == rfield


def test_find_foreign_multiple():
    """test detection of join matching multiple foreign keys"""
    with pytest.raises(TypeError) as ex:
        _find_foreign_key_reference(B, (A, C))
    assert ex.value.args[0] == "'B' has multiple foreign keys that match"


def test_find_primary():
    """test primary key match"""
    table, field = _find_primary_key_reference(A, (B,))
    assert table == B
    assert field == 'a_id'

    assert _find_primary_key_reference(B, (A,)) is None


def test_find_primary_multiple():
    """test multiple primary key matches"""
    with pytest.raises(TypeError):
        _find_primary_key_reference(A, (B, C))


@pytest.mark.parametrize(
    'tab1,tab2,limit,match_col1,match_tab,match_col2', (
        (A, B, None, 'a_id', 'a', 'id'),
        (B, A, None, 'id', 'b', 'a_id'),
        ((A, B), C, None, 'a_id', 'a', 'id'),
        ((A, C), B, 'a', 'a_id', 'a', 'id'),
        ((A, C), B, A, 'a_id', 'a', 'id'),
    )  # pylint: disable=too-many-arguments
)
def test_pair(tab1, tab2, limit, match_col1, match_tab, match_col2):
    """test _pair function"""
    if not isinstance(tab1, tuple):
        tab1 = (tab1,)
    query = [QueryTable(tab) for tab in tab1]
    column1, table2, column2 = _pair(tab2, query, limit)
    assert column1 == match_col1
    assert table2 == match_tab
    assert column2 == match_col2
