import pytest

from aiodb.model.test.test_save import MockCursor, MockTable


@pytest.mark.asyncio
async def test_delete():

    assert True
    cursor = MockCursor()
    o = MockTable(the_key=100, name='foo')

    await o.delete(cursor)
    assert cursor.query == \
        "DELETE FROM 'tester' WHERE 'the_key'=%s"
    assert cursor.query_after == \
        "DELETE FROM 'tester' WHERE 'the_key'=100"
