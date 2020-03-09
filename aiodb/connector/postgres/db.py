import asyncio
from functools import partial
import logging

from fsm.parser import Parser as parser

from aiodb import Cursor
from aiodb.connector.postgres.serializer import to_postgres


log = logging.getLogger(__name__)


class DB:

    def __init__(self, host='postgres', port=5432, user='', password='',
                 database=None, autocommit=None, debug=False,
                 commit=True):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        if commit is False:  # turn off commits (everything is rolled back)
            autocommit = False
        self.autocommit = autocommit
        self.debug = debug
        self.commit = commit

        # self.packet = parser.parse(
        # 'aiodb.connector.mysql.packet.fsm')
        # self.connection = parser.parse(
        # 'aiodb.connector.mysql.connection.fsm')

    async def cursor(self):
        return await PostgresHandler(self).connect(self.host, self.port)


def trace(name, state, event, dflt, is_internal):
    log.debug(f'FSM-{name}: s={state}, e={event}, d={dflt}, i={is_internal}')


class PostgresHandler:

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
            self.close,
            quote='"',
            transactions=db.commit,
        )

    async def connect(self, host, port):
        self.reader, self.writer = await asyncio.open_connection(host, port)
        if not self.fsm.handle('ready'):
            print("unexpected event 'ready'")
            return None
        while not self.is_connected:
            await self.read()
        return self.cursor

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    def send(self, data):
        self.writer.write(data)
        self._send = True

    async def read(self, length=1000):
        data = await self.reader.read(length)
        self.message_fsm.handle('data', data)
        if self._send:
            await self.writer.drain()
            self._send = False

    async def execute(self, query, args=None, is_insert=False, pk=None):
        cur = self.cursor
        ctx = self.fsm.context

        if is_insert:
            query += f' RETURNING "{pk}" AS "last_id"'

        self.fsm.handle('query', query)  # start query running

        while ctx.is_running:  # while query is running
            await self.read()

        if is_insert:
            names, values = ctx.result_set
            cur.last_id = values[0][0]
            return None

        # TODO
        # cur.message = ctx.message

        return ctx.result_set

    def on_message(self, message):
        if not self.fsm.handle(message.name, message):
            print("unexpected event '{}'".format(message.name))

    def on_data(self, data):
        self.message_fsm.handle('data', data)

    @property
    def is_connected(self):
        return self.fsm.context.is_connected
