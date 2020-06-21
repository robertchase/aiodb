"""test fixtures and common code"""
import asyncio
import os
import pytest

from aiodb.connector.mysql.db import DB


@pytest.fixture
def db_defn():
    return DB(
        host=os.getenv('MYSQL_HOST', 'mysql'),
        user='test',
        password=os.getenv('MYSQL_PASSWORD', ''),
        database='test_mysql',
        commit=False,
    )


def run_async(func, *args, **kwargs):
    """run a function in an event loop"""

    async def _fn():
        return await func(*args, **kwargs)

    return asyncio.run(_fn())


@pytest.fixture
def run():
    """handy fixture for async runner"""
    return run_async
