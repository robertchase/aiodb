import datetime
from decimal import Decimal
import pytest

from aiodb.connector.mysql.bit import Bit


async def _handle(cursor, column, value, expect=None):
    insert = f'INSERT INTO `test` (`{column}`)VALUES(%s)'
    await cursor.execute(insert, value)
    print(cursor.query_after)

    select = f'SELECT `{column}` FROM `test`WHERE id=%s'
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
async def test_decimal(cursor):
    await _handle(cursor, 'a_dec', Decimal('123.456'))


@pytest.mark.asyncio
async def test_tinyint_zero(cursor):
    await _handle(cursor, 'b_tin', 0)


@pytest.mark.asyncio
async def test_tinyint_integer(cursor):
    await _handle(cursor, 'b_tin', 10)


@pytest.mark.asyncio
async def test_tinyint_float(cursor):
    await _handle(cursor, 'b_tin', 100.1, expect=100)


@pytest.mark.asyncio
async def test_tinyint_negative(cursor):
    await _handle(cursor, 'b_tin', -10)


@pytest.mark.asyncio
async def test_smallint_zero(cursor):
    await _handle(cursor, 'b_sma', 0)


@pytest.mark.asyncio
async def test_smallint_integer(cursor):
    await _handle(cursor, 'b_sma', 10)


@pytest.mark.asyncio
async def test_smallint(cursor):
    await _handle(cursor, 'b_sma', '100', expect=100)


@pytest.mark.asyncio
async def test_smallint_float(cursor):
    await _handle(cursor, 'b_sma', 100.1, expect=100)


@pytest.mark.asyncio
async def test_smallint_negative(cursor):
    await _handle(cursor, 'b_sma', -10)


@pytest.mark.asyncio
async def test_mediumint_zero(cursor):
    await _handle(cursor, 'b_med', 0)


@pytest.mark.asyncio
async def test_mediumint_integer(cursor):
    await _handle(cursor, 'b_med', 10)


@pytest.mark.asyncio
async def test_mediumint_float(cursor):
    await _handle(cursor, 'b_med', 100.1, expect=100)


@pytest.mark.asyncio
async def test_mediumint_negative(cursor):
    await _handle(cursor, 'b_med', -10)


@pytest.mark.asyncio
async def test_int_zero(cursor):
    await _handle(cursor, 'b_int', 0)


@pytest.mark.asyncio
async def test_int_integer(cursor):
    await _handle(cursor, 'b_int', 10)


@pytest.mark.asyncio
async def test_int_float(cursor):
    await _handle(cursor, 'b_int', 100.1, expect=100)


@pytest.mark.asyncio
async def test_int_negative(cursor):
    await _handle(cursor, 'b_int', -10)


@pytest.mark.asyncio
async def test_big_int_zero(cursor):
    await _handle(cursor, 'b_big', 0)


@pytest.mark.asyncio
async def test_big_int_integer(cursor):
    await _handle(cursor, 'b_big', 10)


@pytest.mark.asyncio
async def test_big_int_float(cursor):
    await _handle(cursor, 'b_big', 100.1, expect=100)


@pytest.mark.asyncio
async def test_big_int_negative(cursor):
    await _handle(cursor, 'b_big', -10)


@pytest.mark.asyncio
async def test_float_zero(cursor):
    await _handle(cursor, 'c_flo', 0)


@pytest.mark.asyncio
async def test_float_one(cursor):
    await _handle(cursor, 'c_flo', 1)


@pytest.mark.asyncio
async def test_float_neg_one(cursor):
    await _handle(cursor, 'c_flo', -1)


@pytest.mark.asyncio
async def test_float_fraction(cursor):
    await _handle(cursor, 'c_flo', 0.1)


@pytest.mark.asyncio
async def test_float_neg_fraction(cursor):
    await _handle(cursor, 'c_flo', -0.1)


@pytest.mark.asyncio
async def test_float_float(cursor):
    await _handle(cursor, 'c_flo', 123.456)


@pytest.mark.asyncio
async def test_float_string(cursor):
    await _handle(cursor, 'c_flo', '123.456', expect=123.456)


@pytest.mark.asyncio
async def test_double_zero(cursor):
    await _handle(cursor, 'd_dou', 0)


@pytest.mark.asyncio
async def test_double_one(cursor):
    await _handle(cursor, 'd_dou', 1)


@pytest.mark.asyncio
async def test_double_neg_one(cursor):
    await _handle(cursor, 'd_dou', -1)


@pytest.mark.asyncio
async def test_double_fraction(cursor):
    await _handle(cursor, 'd_dou', 0.1)


@pytest.mark.asyncio
async def test_double_neg_fraction(cursor):
    await _handle(cursor, 'd_dou', -0.1)


@pytest.mark.asyncio
async def test_double_float(cursor):
    await _handle(cursor, 'd_dou', 123.456)


@pytest.mark.asyncio
async def test_double_float_string(cursor):
    await _handle(cursor, 'd_dou', '123.456', expect=123.456)


@pytest.mark.asyncio
async def test_bit(cursor):
    b = Bit(5)(10)
    await _handle(cursor, 'e_bit', b, expect=10)


@pytest.mark.asyncio
async def test_datetime_string_date(cursor):
    await _handle(cursor,
                  'f_dtm',
                  '2020-01-02',
                  expect=datetime.datetime(2020, 1, 2))


@pytest.mark.asyncio
async def test_datetime_string(cursor):
    await _handle(cursor,
                  'f_dtm',
                  '2020-01-02 12:13:14',
                  expect=datetime.datetime(2020, 1, 2, 12, 13, 14))


@pytest.mark.asyncio
async def test_datetime_datetime(cursor):
    await _handle(cursor,
                  'f_dtf',
                  datetime.datetime(2020, 1, 2, 12, 13, 14))


@pytest.mark.asyncio
async def test_datetime_datetime_with_ms(cursor):
    await _handle(cursor,
                  'f_dtf',
                  datetime.datetime(2020, 1, 2, 12, 13, 14, 321))


@pytest.mark.asyncio
async def test_timestamp_string_date(cursor):
    await _handle(cursor,
                  'f_tms',
                  '2020-01-02',
                  expect=datetime.datetime(2020, 1, 2))


@pytest.mark.asyncio
async def test_timestamp_string(cursor):
    await _handle(cursor,
                  'f_tms',
                  '2020-01-02 12:13:14',
                  expect=datetime.datetime(2020, 1, 2, 12, 13, 14))


@pytest.mark.asyncio
async def test_timestamp_datetime(cursor):
    await _handle(cursor,
                  'f_tms',
                  datetime.datetime(2020, 1, 2, 12, 13, 14))


@pytest.mark.asyncio
async def test_timestamp_datetime_ms(cursor):
    await _handle(cursor,
                  'f_tms',
                  datetime.datetime(2020, 1, 2, 12, 13, 14, 321))


@pytest.mark.asyncio
async def test_date_string(cursor):
    await _handle(cursor,
                  'f_dat',
                  '2020-01-02',
                  expect=datetime.date(2020, 1, 2))


@pytest.mark.asyncio
async def test_date_string_time(cursor):
    await _handle(cursor,
                  'f_dat',
                  '2020-01-02 12:13:14',
                  expect=datetime.date(2020, 1, 2))


@pytest.mark.asyncio
async def test_date_date(cursor):
    await _handle(cursor,
                  'f_dat',
                  datetime.date(2020, 1, 2))


@pytest.mark.asyncio
async def test_time_time(cursor):
    await _handle(cursor, 'f_tim', '12:01:02', expect=datetime.time(12, 1, 2))


@pytest.mark.asyncio
async def test_time_delta(cursor):
    await _handle(cursor,
                  'f_tim',
                  '50:01:02',
                  expect=datetime.timedelta(hours=50, minutes=1, seconds=2))


@pytest.mark.asyncio
async def test_year_string(cursor):
    await _handle(cursor, 'f_yea', '2020', expect=2020)


@pytest.mark.asyncio
async def test_year_integer(cursor):
    await _handle(cursor, 'f_yea', 2020)


@pytest.mark.asyncio
async def test_char(cursor):
    await _handle(cursor, 'g_cha', 'akk')


@pytest.mark.asyncio
async def test_varchar(cursor):
    await _handle(cursor, 'g_vch', 'akk')


@pytest.mark.asyncio
async def test_binary_ascii(cursor):
    await _handle(cursor, 'g_bin', 'akk',
                  expect=b'akk' + b'\x00' * 7)


@pytest.mark.asyncio
async def test_binary_greek(cursor):
    await _handle(cursor, 'g_bin', 'καλι',
                  expect=('καλι'.encode() + b'\x00' * 10)[:10])


@pytest.mark.asyncio
async def test_binary(cursor):
    await _handle(cursor, 'g_bin', b'\x00akk',
                  expect=b'\x00akk' + b'\x00' * 6)


@pytest.mark.asyncio
async def test_varbinary_ascii(cursor):
    await _handle(cursor, 'g_vbi', 'akk', expect=b'akk')


@pytest.mark.asyncio
async def test_varbinary_greek(cursor):
    await _handle(cursor, 'g_vbi', 'καλι', expect='καλι'.encode())


@pytest.mark.asyncio
async def test_varbinary(cursor):
    await _handle(cursor, 'g_vbi', b'\x00akk')


@pytest.mark.asyncio
async def test_tinyblob_ascii(cursor):
    await _handle(cursor, 'g_tbl', 'akk', expect=b'akk')


@pytest.mark.asyncio
async def test_tinyblob_greek(cursor):
    await _handle(cursor, 'g_tbl', 'καλι', expect='καλι'.encode())


@pytest.mark.asyncio
async def test_tinyblob(cursor):
    await _handle(cursor, 'g_tbl', b'\x00akk')


@pytest.mark.asyncio
async def test_blob_ascii(cursor):
    await _handle(cursor, 'g_blo', 'akk', expect=b'akk')


@pytest.mark.asyncio
async def test_blob_greek(cursor):
    await _handle(cursor, 'g_blo', 'καλι', expect='καλι'.encode())


@pytest.mark.asyncio
async def test_blob(cursor):
    await _handle(cursor, 'g_blo', b'\x00akk')


@pytest.mark.asyncio
async def test_mediumblob_ascii(cursor):
    await _handle(cursor, 'g_mbl', 'akk', expect=b'akk')


@pytest.mark.asyncio
async def test_mediumblob_greek(cursor):
    await _handle(cursor, 'g_mbl', 'καλι', expect='καλι'.encode())


@pytest.mark.asyncio
async def test_mediumblob(cursor):
    await _handle(cursor, 'g_mbl', b'\x00akk')


@pytest.mark.asyncio
async def test_longblob_ascii(cursor):
    await _handle(cursor, 'g_lbl', 'akk', expect=b'akk')


@pytest.mark.asyncio
async def test_longblob_greek(cursor):
    await _handle(cursor, 'g_lbl', 'καλι', expect='καλι'.encode())


@pytest.mark.asyncio
async def test_longblob(cursor):
    await _handle(cursor, 'g_lbl', b'\x00akk')


@pytest.mark.asyncio
async def test_tinytext_ascii(cursor):
    await _handle(cursor, 'g_tte', 'akk')


@pytest.mark.asyncio
async def test_tinytext_greek(cursor):
    await _handle(cursor, 'g_tte', 'καλι')


@pytest.mark.asyncio
async def test_tinytext(cursor):
    await _handle(cursor, 'g_tte', b'\x00akk', expect='\x00akk')


@pytest.mark.asyncio
async def test_text_ascii(cursor):
    await _handle(cursor, 'g_tex', 'akk')


@pytest.mark.asyncio
async def test_text_greek(cursor):
    await _handle(cursor, 'g_tex', 'καλι')


@pytest.mark.asyncio
async def test_text(cursor):
    await _handle(cursor, 'g_tex', b'\x00akk', expect='\x00akk')


@pytest.mark.asyncio
async def test_longtext_ascii(cursor):
    await _handle(cursor, 'g_lte', 'akk')


@pytest.mark.asyncio
async def test_longtext_greek(cursor):
    await _handle(cursor, 'g_lte', 'καλι')


@pytest.mark.asyncio
async def test_longtext(cursor):
    await _handle(cursor, 'g_lte', b'\x00akk', expect='\x00akk')


@pytest.mark.asyncio
async def test_enum(cursor):
    await _handle(cursor, 'g_enu', 'three')


@pytest.mark.asyncio
async def test_set(cursor):
    await _handle(cursor, 'g_set', 'one,two')


@pytest.mark.asyncio
async def test_json_dict(cursor):
    await _handle(cursor, 'h_jso', '{"a": 1}', expect=dict(a=1))


@pytest.mark.asyncio
async def test_json_list(cursor):
    await _handle(cursor, 'h_jso', '["a", 1]', expect=['a', 1])
