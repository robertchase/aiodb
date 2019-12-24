class Cursor:

    def __init__(self, execute, close, quote='`', transactions=True):
        """Database cursor

           execute - callable that initiates commands to the database
           close - callable that closes the database connection
           quote - quote character surrounding table/field names
           transactions - if False, don't perform transactions

           Notes:

               1. 'execute' takes a statement with %s substitutions, and
                  an argument, or iterable of arguments, which are
                  transformed/escaped before being substituted in the
                  statement. if no argument is supplied, no substituion
                  occurs.
        """
        self.execute = execute
        self.close = close
        self.quote = quote

        self.last_id = None
        self.message = None
        self.query = None
        self.query_after = None

        self._has_transactions = transactions
        self._transaction_depth = 0

    async def _transaction(self, command):
        if self._has_transactions:
            await self.execute(command)

    async def start_transaction(self):
        self._transaction_depth += 1
        if self._transaction_depth == 1:
            await self._transaction('BEGIN')

    async def commit(self):
        if self._transaction_depth == 0:
            return
        self._transaction_depth -= 1
        if self._transaction_depth == 0:
            await self._transaction('COMMIT')

    async def rollback(self):
        if self._transaction_depth == 0:
            return
        self._transaction_depth = 0
        await self._transaction('ROLLBACK')
