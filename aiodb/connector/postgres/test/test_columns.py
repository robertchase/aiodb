import decimal

import pytest


async def _handle(cursor, column, value, expect=None):
    insert = f'INSERT INTO "test" ("{column}")VALUES(%s)'
    print(insert)
    await cursor.execute(insert, value, is_insert=True, pk='id')
    print(cursor.query_after)

    select = f'SELECT "{column}" FROM "test" WHERE id=%s'
    names, tuples = await cursor.execute(select, cursor.last_id)
    item = tuples[0][0]  # first row, first column

    print(cursor.query_after)
    print('item', type(item), tuples[0])

    if expect is None:
        assert item == value
    else:
        assert item == expect

    await cursor.close()


@pytest.mark.asyncio
async def test_smallint(cursor):
    await _handle(cursor, 'a_sin', 10)


@pytest.mark.asyncio
async def test_int(cursor):
    await _handle(cursor, 'a_int', 10)


@pytest.mark.asyncio
async def test_int_bigint(cursor):
    await _handle(cursor, 'a_bin', 10, expect=10)


@pytest.mark.asyncio
async def test_numeric(cursor):
    await _handle(cursor, 'e_num', 1.234, decimal.Decimal('1.234'))


@pytest.mark.asyncio
async def test_numeric_neg(cursor):
    await _handle(cursor, 'e_num', -1.234, decimal.Decimal('-1.234'))


@pytest.mark.asyncio
async def test_numeric_string(cursor):
    await _handle(cursor, 'e_num', '1.234', decimal.Decimal('1.234'))


@pytest.mark.asyncio
async def test_numeric_2(cursor):
    await _handle(cursor, 'e_nu2', '1.234', decimal.Decimal('1.23'))


@pytest.mark.asyncio
async def test_real(cursor):
    await _handle(cursor, 'f_rea', 1.234)


@pytest.mark.asyncio
async def test_double(cursor):
    await _handle(cursor, 'f_dou', 1.234)


@pytest.mark.asyncio
async def test_smallserial(cursor):
    await _handle(cursor, 'g_sms', 100)


@pytest.mark.asyncio
async def test_serial(cursor):
    await _handle(cursor, 'g_ser', 100)


@pytest.mark.asyncio
async def test_bigserial(cursor):
    await _handle(cursor, 'g_bis', 100)
