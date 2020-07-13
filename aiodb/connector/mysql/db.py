"""mysql connector"""
# pylint: disable=invalid-overridden-method
import asyncio
import logging
import socket

from fsm.parser import Parser as parser

from aiodb import Cursor
from aiodb.model import sync as to_sync
from .serializer import to_mysql
from .handler_mixin import HandlerMixin


log = logging.getLogger(__name__)


class DB:  # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-instance-attributes
    """mysql-specific database connector"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 host='mysql', port=3306, user='', password='',
                 database=None, autocommit=None, isolation=None, debug=False,
                 commit=True, sync=False):
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
        self.sync = sync

        if sync:
            self.cursor = self.sync_cursor
            SyncMysqlHandler.patch()
            to_sync.patch()

        self.packet = parser.parse('aiodb.connector.mysql.packet.fsm')
        self.connection = parser.parse('aiodb.connector.mysql.connection.fsm')

    async def cursor(self):  # pylint: disable=method-hidden
        """return mysql connection"""
        return await MysqlHandler(self).connect(self.host, self.port)

    def sync_cursor(self):
        """return sync mysql connection"""
        return MysqlHandler(self).connect(self.host, self.port)


def trace(state, event, dflt, is_internal):
    """log message on event"""
    msg = f'FSM: s={state}, e={event}, d={dflt}, i={is_internal}'
    log.debug(msg)


def on_undefined(state, event, *args):  # pylint: disable=unused-argument
    """log message on undefined event"""
    msg = f"event '{event}' not handled in state '{state}'"
    log.debug(msg)


class MysqlHandler(HandlerMixin):
    """mysql specific handler"""

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
        self.fsm.undefined = on_undefined
        self._send = False

        self.reader = None
        self.writer = None

        self.cursor = Cursor(
            self.execute,
            to_mysql,
            self.close,
            quote='`',
            transactions=db.commit,
        )

    async def connect(self, host, port):
        """connect to database"""
        self.reader, self.writer = await asyncio.open_connection(host, port)
        while not self.is_connected:
            await self.read()
        return self.cursor

    async def execute(self, query,
                      **kwargs):  # pylint: disable=unused-argument
        """send query to database and wait for response"""
        cur = self.cursor
        ctx = self.fsm.context

        self.fsm.handle('query', query)  # start query running

        while ctx.is_running:  # while query is running
            await self.read()

        cur.last_id = ctx.last_id
        cur.message = ctx.message

        return ctx.result_set

    def on_packet(self, packet, sequence):
        """handle arrival of packet"""
        if not self.fsm.handle('packet', packet, sequence):
            self.close()


class SyncMysqlHandler(MysqlHandler):
    """methods to convert MysqlHandler to sync"""

    @classmethod
    def patch(cls):
        """replace all async methods in MysqlHandler class"""
        MysqlHandler.connect = cls.connect
        MysqlHandler.close = cls.close
        MysqlHandler.send = cls.send
        MysqlHandler.read = cls.read
        MysqlHandler.execute = cls.execute

    def connect(self, host, port):
        """connect to database"""
        self.sock = socket.create_connection((host, port))  # pylint: disable=attribute-defined-outside-init
        while not self.is_connected:
            self.read()
        return self.cursor

    def close(self):
        """close the database connection"""
        self.sock.close()

    def send(self, data):
        """send data to database"""
        self.sock.sendall(data)

    def read(self, length=1000):
        """read up to 1000 bytes from the database"""
        data = self.sock.recv(length)
        self.packet_fsm.handle('data', data)

    def execute(self, query,
                **kwargs):  # pylint: disable=unused-argument
                            # pylint: disable=invalid-overridden-method
        """send query to database and wait for response"""
        cur = self.cursor
        ctx = self.fsm.context

        self.fsm.handle('query', query)  # start query running

        while ctx.is_running:  # while query is running
            self.read()

        cur.last_id = ctx.last_id
        cur.message = ctx.message

        return ctx.result_set
