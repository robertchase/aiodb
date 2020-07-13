"""postgres connector"""
import asyncio
from functools import partial
import logging

from fsm.parser import Parser as parser

from aiodb import Cursor
from aiodb.connector.mysql.handler_mixin import HandlerMixin
from .serializer import to_postgres


log = logging.getLogger(__name__)


class DB:  # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-instance-attributes
    """postgres-specific database connector"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 host='postgres', port=5432, user='', password='',
                 database=None, autocommit=None, debug=False,
                 commit=True):
        self.host = host
        self.port = int(port)
        self.password = password
        self.user = user
        self.database = database
        if commit is False:  # turn off commits (everything is rolled back)
            autocommit = False
        self.autocommit = autocommit
        self.commit = commit
        self.debug = debug

        # self.packet = parser.parse(
        # 'aiodb.connector.mysql.packet.fsm')
        # self.connection = parser.parse(
        # 'aiodb.connector.mysql.connection.fsm')

    async def cursor(self):
        """return postgres connection"""
        return await PostgresHandler(self).connect(self.host, self.port)


def trace(name, state, event, dflt, is_internal):
    """trace function for FSM events"""
    msg = f'FSM-{name}: s={state}, e={event}, d={dflt}, i={is_internal}'
    log.debug(msg)


class PostgresHandler(HandlerMixin):
    """postgres specific handler"""

    def __init__(self, db):
        self.message_fsm = parser.load(
            'aiodb.connector.postgres.message.fsm',
            on_message=self.on_message
        )
        self.fsm = parser.load(
            'aiodb.connector.postgres.connection.fsm',
            send=self.send,
            user=db.user,
            password=db.password,
            database=db.database,
        )
        if db.debug:
            # self.message_fsm.trace = partial(trace, 'MSG')
            self.fsm.trace = partial(trace, 'CON')

        self.cursor = Cursor(
            self.execute,
            to_postgres,
            self.close,
            quote='"',
            transactions=db.commit,
        )
        self._send = False
        self.reader = None
        self.writer = None

    async def connect(self, host, port):
        """connect to database"""
        self.reader, self.writer = await asyncio.open_connection(host, port)
        if not self.fsm.handle('ready'):
            log.error("unexpected event 'ready'")
            return None
        while not self.is_connected:
            await self.read()
        return self.cursor

    async def execute(self, query, is_insert=False, primary_key=None):
        """send query to database and wait for response"""
        cur = self.cursor
        ctx = self.fsm.context

        if is_insert:
            query += f' RETURNING "{primary_key}" AS "last_id"'

        self.fsm.handle('query', query)  # start query running

        while ctx.is_running:  # while query is running
            await self.read()

        if is_insert:
            _, values = ctx.result_set
            cur.last_id = values[0][0]
            return None

        # TODO
        # cur.message = ctx.message

        return ctx.result_set

    def on_message(self, message):
        """handle an incoming message

           messages are constructed from data arriving in 'on_data'
        """
        if not self.fsm.handle(message.name, message):
            print("unexpected event '{}'".format(message.name))

    def on_data(self, data):
        """handle new data arrival"""
        self.message_fsm.handle('data', data)
