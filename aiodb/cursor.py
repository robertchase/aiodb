"""generic cursor"""


class Raw:  # pylint: disable=too-few-public-methods
    """raw sql string"""

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        return f"Raw('{self.data}')"


class Cursor:  # pylint: disable=too-many-instance-attributes
    """abstract cursor class"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 execute, ping, close, serialize, last_id, last_message,
                 quote='`', transactions=True):
        """Database cursor

           Abstract interface to a database. A cursor represents one
           connection to a database. After close is called, the connection is
           no longer active.

           Arguments:

                execute - callable that executes commands against the database

                    Definition:
                        async def execute(query, **kwargs)

                    Arguments:
                        query  - escaped sql command string
                        kwargs - database specific

                    Result:
                        columns - list of column names (may be empty)
                        rows    - list of row tuples (may be empty)

                ping - callable that verifies the database connection

                    async def ping()
                    Return:
                        bool

                close - callable that closes the database connection

                    async def close()
                    Return:
                        None

                serialize - callable that escapes inputs

                    Definition:
                        serialize(value) => escaped_value

                last_id - callable that returns the last auto-generated id from
                          the most recent query

                last_message - callable that returns the message from the most
                               recent query

                quote - quote character surrounding table/field names

                transactions - if False, don't perform transactions
        """
        self._execute = execute
        self.ping = ping
        self.close = close
        self.serialize = serialize
        self.last_id = last_id
        self.last_message = last_message
        self.quote = quote

        self.query = None
        self.query_after = None

        self._has_transactions = transactions
        self._transaction_depth = 0

    async def _transaction(self, command):
        if self._has_transactions:
            await self.execute(command)

    async def start_transaction(self):
        """Start database transaction
        """
        self._transaction_depth += 1
        if self._transaction_depth == 1:
            await self._transaction('BEGIN')

    async def commit(self):
        """Commit database transaction
        """
        if self._transaction_depth == 0:
            return
        self._transaction_depth -= 1
        if self._transaction_depth == 0:
            await self._transaction('COMMIT')

    async def rollback(self):
        """Rollback database transaction
        """
        if self._transaction_depth == 0:
            return
        self._transaction_depth = 0
        await self._transaction('ROLLBACK')

    async def execute(self, query, args=None, **kwargs):
        """Execute an arbitrary SQL command

           The query is a string with %s subsitiutions which are replaced
           with args after they have been converted and escaped. After
           substitution, the query is passed to the database-specific
           execute function specified in __init__.

           Parameters:
               query  - query string (with %s substitutions)
               args   - substitution parameters
               kwargs - database specific

          Result:
              Same as result of execute function specified in __init__.
        """
        self.query = query
        self.query_after = None

        def _serialize(item):
            if isinstance(item, Raw):
                return item.data
            return self.serialize(item)

        if args is not None:
            if isinstance(args, (list, tuple)):
                args = tuple([_serialize(arg) for arg in args])
            else:
                args = _serialize(args)
            query = query % args

        self.query_after = query
        return await self._execute(query, **kwargs)

    async def select(self, query, args=None, one=False):
        """Run an arbitrary select statement

            The result is a list of objects, each of which contains the data
            in a single row of the query. Each row-object allows column access
            using dot or bracket notation.

            Parameters:
                query - query string (with %s substitutions)
                args  - substitution parameters
                        (None, scalar or tuple)
                one   - if True, return first row in result instead of a
                        list of rows

            Result:
                list of row-objects (or single row-object) or None
        """
        columns, rows = await self.execute(query, args=args)

        class Row:
            """represent a row of a resultset"""

            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

            def __getitem__(self, name):
                return self.__dict__[name]

            def __repr__(self):
                return str(self.__dict__)

        if rows:
            result = [Row(**(dict(zip(columns, row)))) for row in rows]
            if one and result:
                return result[0]
            return result
