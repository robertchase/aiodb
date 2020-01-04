class Cursor:

    def __init__(self, execute, close, quote='`', transactions=True):
        """Database cursor

           Abstract interface to a database. A cursor represents one
           connection to a database. After close is called, the connection is
           no longer active.

           execute - callable that executes commands against the database

             async def execute(query, args, **kwargs)

               Parameters:
                 query  - sql command string
                 args   - substitution parameters (Note 1)
                 kwargs - additional input (varies by database)

               Result:
                 columns - list of column names (may be empty)
                 rows    - list of row tuples (may be empty)

               Notes:
                 1. The cursor.query attribute holds the last query passed
                    to the execute callable.
                 2. The cursor.query_after attributes holds the last query
                    passed to the execute callable with all args substituted
                    into the string.
                 3. If the most recent call to execute was an INSERT into
                    a table that supports an auto-increment type field,
                    the cursor.last_id attribute will contain the value
                    inserted in this field.
                 4. Any message associated with the most recent call to
                    execute is held in the cursor.message attribute.

           close - callable that closes the database connection

             async def close()
               Return:
                   None

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

            Notes:
                1. a column name is either the value specified in the
                    query 'AS' clause, or the value used to indicate the
                    select_expr
        """
        class Row:

            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

            def __getitem__(self, name):
                return self.__dict__[name]

            def __repr__(self):
                return str(self.__dict__)

        columns, rows = await self.execute(query, args=args)
        if rows:
            result = [Row(**(dict(zip(columns, row)))) for row in rows]
            if one and len(result):
                return result[0]
            return result