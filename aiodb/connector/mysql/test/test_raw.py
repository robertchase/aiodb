"""validate operation of Raw strings"""
import re

from aiodb import Raw


def test_raw(db_defn, run):
    """see if Raw strings go through without escaping"""

    async def _test_raw(db_defn):
        cursor = await db_defn.cursor()
        insert = 'INSERT INTO `test` (`g_vch`) VALUES (%s)'
        await cursor.execute(insert, 'NOW()')
        select = 'SELECT `id`, `g_vch` FROM `test`'
        result = await cursor.select(select, one=True)
        assert result['g_vch'] == 'NOW()'

        await cursor.execute(insert, Raw('NOW()'))
        key = cursor.last_id
        select = 'SELECT `g_vch` FROM `test` where id=%s'
        result = await cursor.select(select, key, one=True)
        assert re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
                        result['g_vch'])

        await cursor.close()

    run(_test_raw, db_defn)
