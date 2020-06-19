from aiodb import Model, Field, Integer

from aiodb.model.test.conftest import run_async


class MockTable(Model):

    __TABLENAME__ = 'tester'

    the_key = Field(Integer, is_primary=True)
    name = Field()
    yeah = Field(is_nullable=True)


def test_return(cursor):
    """verify that save returns the model"""

    o = MockTable(name='test')

    # insert
    result = run_async(o.save, cursor)
    cursor._execute.assert_called_once()
    assert o == result

    cursor._execute.reset_mock()
    o.the_key = 10  # give it a PK

    # update with no changes
    result = run_async(o.save, cursor)
    cursor._execute.assert_not_called()
    assert o == result


def test_insert(cursor):
    """verify that before and after query reflect an insert"""

    o = MockTable(name='test')
    run_async(o.save, cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( %s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( test )"


def test_insert_multiple(cursor):
    """verify insert with multiple values"""

    o = MockTable(name='test', yeah='lala')
    run_async(o.save, cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( %s,%s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( test,lala )"


def test_quote(cursor):
    """test that cursor quote value shows up in insert statements"""

    cursor.quote = '!'
    o = MockTable(name='test')
    run_async(o.save, cursor)
    assert cursor.query == \
        "INSERT INTO !tester! ( !name! ) VALUES ( %s )"


def test_insert_pk(cursor):
    """make sure that inserted object is updated with key

       the save method clears cursor.last_id before calling execute. after
       execute it uses cursor.last_id to update the primary key of an inserted
       object.
    """
    ID = 100

    async def execute(*args, **kwargs):
        """set last_id during execute call, since save cleared it"""
        cursor.last_id = ID

    o = MockTable(name='test', yeah='lala')
    cursor._execute = execute
    run_async(o.save, cursor)
    assert o.the_key == ID


def test_update(cursor):
    """verify update"""

    o = MockTable(the_key=10, name='a', yeah='a')
    o.name = 'test'
    o.yeah = 'lala'
    run_async(o.save, cursor)
    assert cursor.query == \
        "UPDATE  'tester' SET 'name'=%s,'yeah'=%s WHERE  'the_key'=%s"
    assert cursor.query_after == \
        "UPDATE  'tester' SET 'name'=test,'yeah'=lala WHERE  'the_key'=10"


def test_insert_updated(cursor):
    """verify empty updated attribute on insert"""

    o = MockTable(name='a', yeah='a')
    run_async(o.save, cursor)
    assert o._updated == []


def test_update_updated(cursor):
    """verify updated attribute"""

    o = MockTable(the_key=10, name='a', yeah='a')

    o.name = 'test'
    run_async(o.save, cursor)
    assert o._updated == ['name']

    o.name = 'foo'
    o.yeah = 'bar'
    run_async(o.save, cursor)
    assert o._updated == ['name', 'yeah']

    run_async(o.save, cursor)  # nothing changed
    assert o._updated == []
