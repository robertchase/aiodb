from aiodb.model.cursor import SyncCursor
from aiodb.model.model import SyncModel
from aiodb.model.query import SyncQuery


def patch():
    SyncCursor.patch()
    SyncModel._patch()
    SyncQuery.patch()
