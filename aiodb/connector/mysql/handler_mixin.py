"""common handler methods"""


class HandlerMixin:
    """generic handler methods"""

    async def close(self):
        """close the database connection"""
        self.writer.close()
        await self.writer.wait_closed()

    def send(self, data):
        """send data to database"""
        self.writer.write(data)
        self._send = True

    async def read(self, length=1000):
        """read up to 1000 bytes from the database"""
        data = await self.reader.read(length)
        self.packet_fsm.handle('data', data)
        if self._send:
            await self.writer.drain()
            self._send = False

    @property
    def is_connected(self):
        """return True if connected to database"""
        return self.fsm.context.is_connected
