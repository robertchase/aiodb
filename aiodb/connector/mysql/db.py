import asyncio
import logging

from fsm.parser import Parser as parser

from .converters import to_mysql
from aiodb import Cursor


log = logging.getLogger(__name__)


class DB:

    def __init__(self, host='mysql', port=3306, user='', password='',
                 database=None, autocommit=None, isolation=None, debug=False,
                 commit=True):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        if commit is False:  # turn off commits (everything is rolled back)
            autocommit = False
        self.autocommit = autocommit
        self.isolation = isolation
        self.debug = debug
        self.commit = commit

        self.packet = parser.parse('aiodb.connector.mysql.packet.fsm')
        self.connection = parser.parse('aiodb.connector.mysql.connection.fsm')

    async def cursor(self):
        return await MysqlHandler(self).connect(self.host, self.port)


def trace(state, event, dflt, is_internal):
    log.debug(f'FSM: s={state}, e={event}, d={dflt}, i={is_internal}')


class MysqlHandler:

    def __init__(self, db):
        self.packet_fsm = db.packet.compile(
            on_packet=self.on_packet,
        )
        self.fsm = db.connection.compile(
            send=self.send,
            user=db.user,
            password=db.password,
            database=db.database,
            autocommit=db.autocommit,
            isolation=db.isolation,
        )
        if db.debug:
            self.fsm.trace = trace
        self.fsm.undefined = self.on_undefined
        self._send = False

        self.cursor = Cursor(
            self.execute,
            self.close,
            quote='`',
            transactions=db.commit,
        )

    async def connect(self, host, port):
        self.reader, self.writer = await asyncio.open_connection(host, port)
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
        self.packet_fsm.handle('data', data)
        if self._send:
            await self.writer.drain()
            self._send = False

    def on_undefined(self, state, event, *args):
        log.debug(f"event '{event}' not handled in state '{state}'")

    async def execute(self, query, args=None):
        cur = self.cursor
        ctx = self.fsm.context

        cur.query = query
        cur.query_after = None
        if args is not None:
            if isinstance(args, (list, tuple)):
                if len(args) == 1:
                    args = to_mysql(args[0])
                else:
                    args = tuple([to_mysql(arg) for arg in args])
            else:
                args = to_mysql(args)
            query = query % args
        cur.query_after = query

        self.fsm.handle('query', query)  # start query running

        while ctx.is_running:  # while query is running
            await self.read()

        cur.last_id = ctx.last_id
        cur.message = ctx.message

        return ctx.result_set

    def on_packet(self, packet, sequence):
        if not self.fsm.handle('packet', packet, sequence):
            self.close()

    @property
    def is_connected(self):
        return self.fsm.context.is_connected
