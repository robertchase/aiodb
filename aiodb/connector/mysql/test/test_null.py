"""test operations with null"""


def test_null(db_defn, run):
    """verify null column comes back as None"""

    async def _test_null(db_defn):
        cursor = await db_defn.cursor()
        insert = 'INSERT INTO `test` (`a_dec`) VALUES (%s)'
        await cursor.execute(insert, 10)
        select = 'SELECT `id`, `a_dec`, `b_tin` FROM `test`'
        result = await cursor.select(select, one=True)
        assert result['a_dec'] == 10
        assert result['b_tin'] is None
        await cursor.close()

    run(_test_null, db_defn)
