async def execute(cursor, query, args=None):
    """Run an arbitrary statement

        The objects returned in the result do not have any of the
        functionality of a DAO. Columns are accessed as attributes
        by name using dot or bracket notation.

        Parameters:
            cursor   - database cursor
            query    - query string (with %s substitutions)
            args     - substitution parameters
                        (None, scalar or tuple)

        Result:
            list of Row objects or None

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

    columns, rows = await cursor.execute(query, args=args)
    if columns:
        return [Row(**(dict(zip(columns, row)))) for row in rows]
