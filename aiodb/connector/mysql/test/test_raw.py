import pytest
import re

from aiodb import Raw


@pytest.mark.asyncio
async def test_raw(cursor):
    insert = 'INSERT INTO `test` (`g_vch`) VALUES (%s)'
    await cursor.execute(insert, 'NOW()')
    select = 'SELECT `id`, `g_vch` FROM `test`'
    result = await cursor.select(select, one=True)
    assert result['g_vch'] == 'NOW()'

    await cursor.execute(insert, Raw('NOW()'))
    id = cursor.last_id
    select = 'SELECT `g_vch` FROM `test` where id=%s'
    result = await cursor.select(select, id, one=True)
    assert re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', result['g_vch'])

    await cursor.close()
