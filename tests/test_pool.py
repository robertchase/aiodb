"""test connection pool"""
import asyncio
from unittest import mock

from aiodb.pool import Pool


def test_empty():
    """empty pool"""
    pool = Pool(None)
    assert len(pool.pool) == 0


def test_one():
    """pool with single connection"""

    async def test():
        connector = mock.AsyncMock(return_value=mock.Mock())
        pool = await Pool.setup(connector, size=1)
        assert len(pool.pool) == 1
        connector.assert_called_once()

    asyncio.run(test())


def test_five():
    """pool with five connections"""

    async def test():
        connector = mock.AsyncMock(return_value=mock.Mock())
        pool = await Pool.setup(connector, size=5)
        assert len(pool.pool) == 5
        assert connector.call_count == 5

    asyncio.run(test())


def mock_connector(ping=True):
    """fake database connection factory"""
    async def _connector():
        con = mock.Mock()
        con.ping = mock.AsyncMock(return_value=ping)
        con.close = mock.AsyncMock()
        return con
    return _connector


def test_cursor():
    """test cursor functionality"""

    async def test():
        pool = await Pool.setup(mock_connector(), size=1)
        assert len(pool.pool) == 1
        con = await pool.cursor()
        assert len(pool.pool) == 0
        assert con.pool_index == 1
        con.ping.assert_called_once()

    asyncio.run(test())


def test_failed_ping():
    """test reconnect functionality"""

    async def test():
        pool = await Pool.setup(mock_connector(False), size=1)
        orig = pool.pool[0]
        con = await pool.cursor()
        assert len(pool.pool) == 0
        assert con.pool_index == 1
        assert orig != con
        con.ping.assert_not_called()

    asyncio.run(test())


def test_close():
    """test that close connection returns to pool"""

    async def test():
        pool = await Pool.setup(mock_connector())
        size = len(pool.pool)
        con = await pool.cursor()
        assert len(pool.pool) == size - 1
        await con.close()
        assert len(pool.pool) == size

    asyncio.run(test())
