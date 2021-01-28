"""test fixtures and common code"""
import asyncio
from unittest import mock

import pytest

from aiodb.cursor import Cursor as abstract_cursor


@pytest.fixture
def cursor():
    """return a real cursor with mocked attributes"""
    return abstract_cursor(
        execute=mock.AsyncMock(return_value=((),())),
        ping=mock.AsyncMock(),
        close=mock.AsyncMock(),
        serialize=str,
        last_id=mock.Mock(return_value=100),
        last_message=mock.Mock(return_value="test message"),
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
