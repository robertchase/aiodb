import os
import pytest

from aiodb.connector.postgres.db import DB


@pytest.fixture
async def cursor():
    db = DB(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        user='test',
        password=os.getenv('POSTGRES_PASSWORD', ''),
        database='test_postgres',
        commit=False,
        debug=True,
    )
    return await db.cursor()
