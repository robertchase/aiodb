import pytest


@pytest.mark.asyncio
async def test_null(cursor):
    insert = 'INSERT INTO `test` (`a_dec`) VALUES (%s)'
    await cursor.execute(insert, 10)
    select = 'SELECT `id`, `a_dec`, `b_tin` FROM `test`'
    result = await cursor.select(select, one=True)
    assert result['a_dec'] == 10
    assert result['b_tin'] is None
    await cursor.close()
