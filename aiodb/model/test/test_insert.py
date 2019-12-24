import pytest

from aiodb.model.test.test_save import MockCursor, MockTable


@pytest.mark.asyncio
async def test_insert():

    ID = 100
    cursor = MockCursor()
    o = MockTable(name='test')
    await o.insert(cursor, ID)
    assert o.the_key == ID
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,100 )"


@pytest.mark.asyncio
async def test_insert_pk():

    cursor = MockCursor()
    o = MockTable(the_key=1000, name='test')
    await o.insert(cursor)
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,1000 )"
