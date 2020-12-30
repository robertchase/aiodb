"""connection pool logic"""
import logging


log = logging.getLogger(__name__)


class Pool:
    """connection pool"""

    def __init__(self):
        self.connector = None
        self.pool = []

    @classmethod
    async def setup(cls, connector, size=10):
        """setup a new connection pool

           connector is an async function that returns one open connection to
                     the database. a connection must support a ping():bool and
                     a close() method in order to be managed by the pool.

           size is the number of connections in the pool
        """
        self = cls()
        self.connector = connector

        for index in range(size):
            log.debug("creating pooled connection %d", index + 1)
            con = await connector()
            con.pool_index = index + 1
            con.close = self._pooled_connection_close(con)  # pylint: disable=protected-access
            self.pool.insert(0, con)

        return self

    async def cursor(self):
        """return a database connection

           if all connections in the pool are in use, a new connection will be
           temporarily established
        """
        try:
            connection = self.pool.pop()
            if not await connection.ping():
                connection = await self._reconnect(connection)
            log.debug("using pooled connection %d", connection.pool_index)
        except IndexError:
            log.debug("connection pool exhausted")
            connection = await self.connector()
        return connection

    def _pooled_connection_close(self, connection):
        """replace connection with a new connection and return it to the pool
        """
        connection._close = connection.close  # pylint: disable=protected-access

        async def _close():
            log.debug("replacing pooled connection %d", connection.pool_index)
            try:
                await connection._close()  # pylint: disable=protected-access
                con = await self.connector()
                con.pool_index = connection.pool_index
                con.close = self._pooled_connection_close(con)
                self.pool.insert(0, con)
            except Exception:  # pylint: disable=broad-except
                self.pool.insert(0, connection)

        return _close

    async def _reconnect(self, connection):
        """reconnect when a connection fails a ping()"""
        log.debug("reconnecting pooled connection %d", connection.pool_index)
        try:
            await connection._close()  # pylint: disable=protected-access
            con = await self.connector()
            con.pool_index = connection.pool_index
            con.close = self._pooled_connection_close(con)
            return con
        except Exception:  # pylint: disable=broad-except
            self.pool.insert(0, connection)
