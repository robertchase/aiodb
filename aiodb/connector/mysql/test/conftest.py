import os
import pytest

from aiodb.connector.mysql.db import DB


@pytest.fixture
async def cursor():
    db = DB(
        host=os.getenv('MYSQL_HOST', 'mysql'),
        user='test',
        password=os.getenv('MYSQL_PASSWORD', ''),
        database='test_mysql',
        commit=False,
    )
    return await db.cursor()
