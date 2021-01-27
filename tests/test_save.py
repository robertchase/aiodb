"""test save operations"""
# pylint: disable=protected-access
from unittest import mock

from aiodb import Model, Field, Integer

from tests.conftest import run_async


class MockTable(Model):
    """test model"""

    class Meta:  # pylint: disable=too-few-public-methods,missing-class-docstring
        table_name = "tester"

    the_key = Field(Integer, is_primary=True)
    name = Field()
    yeah = Field(is_nullable=True)


def test_return(cursor):
    """verify that save returns the model"""

    test = MockTable(name='test')

    # insert
    result = run_async(test.save, cursor)
    cursor._execute.assert_called_once()
    assert test == result

    cursor._execute.reset_mock()
    test.the_key = 10  # give it a PK

    # update with no changes
    result = run_async(test.save, cursor)
    cursor._execute.assert_not_called()
    assert test == result


def test_insert(cursor):
    """verify that before and after query reflect an insert"""

    test = MockTable(name='test')
    run_async(test.save, cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( %s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( test )"


def test_insert_multiple(cursor):
    """verify insert with multiple values"""

    test = MockTable(name='test', yeah='lala')
    run_async(test.save, cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( %s,%s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( test,lala )"


def test_quote(cursor):
    """test that cursor quote value shows up in insert statements"""

    cursor.quote = '!'
    test = MockTable(name='test')
    run_async(test.save, cursor)
    assert cursor.query == \
        "INSERT INTO !tester! ( !name! ) VALUES ( %s )"


def test_insert_pk(cursor):
    """make sure that inserted object is updated with key

       the save method clears cursor.last_id before calling execute. after
       execute it uses cursor.last_id to update the primary key of an inserted
       object.
    """
    primary_key = 123

    test = MockTable(name='test', yeah='lala')
    cursor.last_id = mock.Mock(return_value=primary_key)
    run_async(test.save, cursor)
    assert test.the_key == primary_key


def test_update(cursor):
    """verify update"""

    test = MockTable(the_key=10, name='a', yeah='a')
    test.name = 'test'
    test.yeah = 'lala'
    run_async(test.save, cursor)
    assert cursor.query == \
        "UPDATE  'tester' SET 'name'=%s,'yeah'=%s WHERE  'the_key'=%s"
    assert cursor.query_after == \
        "UPDATE  'tester' SET 'name'=test,'yeah'=lala WHERE  'the_key'=10"


def test_insert_updated(cursor):
    """verify empty updated attribute on insert"""

    test = MockTable(name='a', yeah='a')
    run_async(test.save, cursor)
    assert test._updated == {}


def test_update_updated(cursor):
    """verify updated attribute"""

    test = MockTable(the_key=10, name='a', yeah='a')

    test.name = 'test'
    run_async(test.save, cursor)
    assert test._updated == {'name': ('a', 'test')}

    test.name = 'foo'
    test.yeah = 'bar'
    run_async(test.save, cursor)
    assert test._updated == {'name': ('test', 'foo'), 'yeah': ('a', 'bar')}

    run_async(test.save, cursor)  # nothing changed
    assert test._updated == {}
