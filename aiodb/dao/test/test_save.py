import pytest

from aiodb import DAO, Field, Integer


class MockCursor:

    def __init__(self, quote="'", last_id=0):
        self.quote = quote
        self._last_id = last_id

    async def execute(self, stmt, args=None):
        self.query = stmt
        if args is not None:
            if isinstance(args, list):
                args = tuple(args)
            stmt = stmt % args
        self.query_after = stmt
        self.last_id = self._last_id


class MockTable(DAO):

    __TABLENAME__ = 'tester'

    the_key = Field(Integer, is_primary=True)
    name = Field()
    yeah = Field(is_nullable=True)


@pytest.mark.asyncio
async def test_return():

    cursor = MockCursor(last_id=10)
    o = MockTable(name='test')

    result = await o.save(cursor)
    assert cursor.query is not None  # save happened
    assert o == result

    result = await o.save(cursor)
    assert cursor.query is None  # nothing to save
    assert o == result


@pytest.mark.asyncio
async def test_insert():

    cursor = MockCursor()
    o = MockTable(name='test')
    await o.save(cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( %s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name' ) VALUES ( test )"


@pytest.mark.asyncio
async def test_insert_2():

    cursor = MockCursor()
    o = MockTable(name='test', yeah='lala')
    await o.save(cursor)
    assert cursor.query == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( %s,%s )"
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','yeah' ) VALUES ( test,lala )"


@pytest.mark.asyncio
async def test_quote():

    cursor = MockCursor(quote='!')
    o = MockTable(name='test')
    await o.save(cursor)
    assert cursor.query == \
        "INSERT INTO !tester! ( !name! ) VALUES ( %s )"


@pytest.mark.asyncio
async def test_insert_pk():

    ID = 100
    cursor = MockCursor(last_id=ID)
    o = MockTable(name='test', yeah='lala')
    await o.save(cursor)
    assert o.the_key == ID


@pytest.mark.asyncio
async def test_update():

    cursor = MockCursor()
    o = MockTable(the_key=10, name='a', yeah='a')
    o.name = 'test'
    o.yeah = 'lala'
    await o.save(cursor)
    assert cursor.query == \
        "UPDATE  'tester' SET 'name'=%s,'yeah'=%s WHERE  'the_key'=%s"
    assert cursor.query_after == \
        "UPDATE  'tester' SET 'name'=test,'yeah'=lala WHERE  'the_key'=10"


@pytest.mark.asyncio
async def test_save_updated():

    cursor = MockCursor()
    o = MockTable(name='a', yeah='a')
    await o.save(cursor)
    assert o._updated == []


@pytest.mark.asyncio
async def test_update_updated():

    cursor = MockCursor(last_id=10)
    o = MockTable(name='a', yeah='a')
    await o.save(cursor)  # insert
    assert o._updated == []

    o.name = 'test'
    await o.save(cursor)
    assert o._updated == ['name']

    await o.save(cursor)  # nothing changed
    assert o._updated == []
