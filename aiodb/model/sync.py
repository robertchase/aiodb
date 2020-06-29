"""swap to sync mode"""
from aiodb.model.cursor import Cursor
from aiodb.model.model import Model
from aiodb.model.query import Query


def patch():
    """call each module to patch async functions with non-async ones"""
    Cursor.patch()
    Model._patch()  # pylint: disable=protected-access
    Query.patch()
