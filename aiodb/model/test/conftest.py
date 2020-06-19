import asyncio
from unittest import mock

import pytest

from aiodb.model.cursor import Cursor as abstract_cursor


@pytest.fixture
def cursor():
    """return a real cursor with mocked attributes"""
    return abstract_cursor(
        execute=mock.AsyncMock(),
        serialize=str,
        close=mock.AsyncMock(),
        quote="'",
        transactions=False,
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
