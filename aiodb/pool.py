"""connection pool logic"""
import logging


log = logging.getLogger(__name__)


class DatabaseReconnectError(Exception):
    """unable to reconnect to database"""


class Pool:
    """connection pool"""

    def __init__(self, connector):
        self.connector = connector
        self.pool = []

    @classmethod
    async def setup(cls, connector, size=10):
        """setup a new connection pool

           connector is an async function that returns one open connection to
                     the database. a connection must support a ping():bool and
                     a close() method in order to be managed by the pool.

           size is the number of connections in the pool
        """
        self = cls(connector)

        for index in range(size):
            log.debug("creating pooled connection %d", index + 1)
            con = await connector()
            con.pool_index = index + 1
            con.close = self.pooled_connection_close(con)
            self.pool.insert(0, con)

        return self

    async def cursor(self):
        """return a database connection

           if all connections in the pool are in use, a new on-demand
           connection will be established, but not added to the pool
        """
        try:
            connection = self.pool.pop()
            if not await connection.ping():
                connection = await self._reconnect(connection)
            log.debug("using pooled connection %d", connection.pool_index)
        except IndexError:
            log.debug("connection pool exhausted")
            connection = await self.connector()
        except DatabaseReconnectError:
            self.pool.insert(0, connection)
            raise
        return connection

    async def _replace(self, connection):
        """replace connection with a new connection"""
        await connection.raw_close()
        con = await self.connector()
        con.pool_index = connection.pool_index
        con.close = self.pooled_connection_close(con)
        return con

    def pooled_connection_close(self, connection):
        """close connection and return it to the pool"""
        connection.raw_close = connection.close

        async def _close():
            """close connection and replace with new connection"""
            try:
                log.debug("replacing pooled connection %d",
                          connection.pool_index)
                con = await self._replace(connection)
                self.pool.insert(0, con)
            except Exception:  # pylint: disable=broad-except
                # something didn't work, put the old connection in the pool
                self.pool.insert(0, connection)

        return _close

    async def _reconnect(self, connection):
        """reconnect a pooled connection"""
        log.debug("reconnecting pooled connection %d", connection.pool_index)
        try:
            return await self._replace(connection)
        except Exception as exc:  # pylint: disable=broad-except
            raise DatabaseReconnectError(str(exc))
