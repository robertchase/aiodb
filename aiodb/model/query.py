"""query helper

The MIT License (MIT)
https://github.com/robertchase/aiodb/blob/master/LICENSE.txt
"""
# pylint: disable=protected-access
import inspect
from aiodb.util import import_by_path, snake_to_camel


class Query:
    """query constructor"""

    def __init__(self, table):
        tab = QueryTable(table)
        self._tables = [tab]
        self._where = None
        self._order = None

    def where(self, where=None):
        """accept where clause"""
        self._where = where
        return self

    def order(self, order):
        """accept order by clause"""
        self._order = order
        return self

    def join(self, table, table2=None, alias=None, outer=None):
        """Add a table to the query

           The table will be joined to another table in the query using
           foreign or primary key matches.

            Parameters:
                table  - Model of the table to add to the query (1)(4)
                table2 - path, alias or Model of table to join (2)
                alias  - name of joined table (3)
                outer  - OUTER join indicator
                         LEFT or RIGHT

           Notes:
               1. The table can be specified as a Model or as a dot separated
                  path to a Model for import. First, foreign keys in 'table'
                  will be checked for a single matching primary key in one
                  of the tables that is already part of the query. If no
                  match is found, the primary key of 'table' will be matched
                  in the same way.
               2. If multiple matches occur when trying to join 'table',
                  the ambiguity can be removed by specifying which existing
                  table to match. Foreign keys from 'table' will be checked
                  first, followed by the primary key.
               3. The 'alias' parameter is used to allow the same Model to be
                  joined more than once.
               4. Any joined Model is accesible using square bracket notation
                  on the main Query object. The default attribute name is the
                  lower case classname of the Model. Specifying 'alias' will
                  override this default.

                  Example of join result structure:

                      This query:

                        Root.query().join(Node).execute(...)

                      will result in a set of Root instances, each joined
                      to a Node instance. Each Node instance is added as
                      an attribute of a Root instance. Therefore:

                        root = result[0]
                        node = root['node']

                  In the case of multiple join clauses, each joined instance
                  will be added to the main Query object.
        """
        try:
            table = import_by_path(table)
        except ValueError as exc:
            raise TypeError(
                "invalid path to table: '{}'".format(table)) from exc
        except ModuleNotFoundError as exc:
            raise TypeError("unable to load '{}'".format(table)) from exc

        field, table2, field2 = _pair(table, self._tables, table2)

        if outer is None:
            join = 'JOIN'
        elif outer.lower() == 'right':
            join = 'RIGHT OUTER JOIN'
        elif outer.lower() == 'left':
            join = 'LEFT OUTER JOIN'
        else:
            raise ValueError("invalid outer join value: '{}'".format(outer))

        tab = QueryTable(table, alias, field, join, table2, field2)
        if tab.alias in [t.alias for t in self._tables]:
            raise ValueError(f"duplicate table '{tab.alias}'")
        self._tables.append(tab)

        return self

    def _prepare(self,  # pylint: disable=too-many-arguments
                 one, limit, offset, for_update, quote):
        if one and limit:
            raise Exception('one and limit parameters are mutually exclusive')
        if one:
            limit = 1

        stmt = 'SELECT '
        stmt += ', '.join(
            '{column} AS {cnt}_{alias}'.format(
                cnt=cnt, column=_column(table, fld, quote), alias=fld.name)
            for cnt, table in enumerate(self._tables)
            for fld in table._fields()
        )
        stmt += ' FROM '
        stmt += ' '.join(table.join(quote) for table in self._tables)
        if self._where:
            self._where = self._where.replace('{TABLE.', '{TABLE_')
            subs = dict(Q=quote)
            for qot in self._tables:
                subs['TABLE_' + qot.table_name] = quote + qot.alias + quote
            stmt += ' WHERE ' + self._where.format(**subs)
        if self._order:
            stmt += ' ORDER BY ' + self._order
        if limit:
            stmt += ' LIMIT %d' % int(limit)
        if offset:
            stmt += ' OFFSET %d' % int(offset)
        if for_update:
            stmt += ' FOR UPDATE'
        return stmt

    async def execute(self,  # pylint: disable=too-many-arguments
                      # pylint: disable=too-many-locals
                      cursor, args=None, one=False, limit=None,
                      offset=None, for_update=False):
        """execute query against database"""

        stmt = self._prepare(one, limit, offset, for_update, cursor.quote)
        columns, values = await cursor.execute(stmt, args)
        columns = [col.split('_', 1)[1] for col in columns]

        rows = []
        for rowdata in values:
            tables = None
            row = list(zip(columns, rowdata))
            for table in self._tables:
                val = dict(row[:table.column_count])
                if val[table._primary().name] is None:
                    obj = None
                else:
                    obj = table.cls(**val)
                if tables is None:
                    primary_table = obj
                    obj._s.tables = tables = {}
                else:
                    tables[table.alias] = obj
                row = row[table.column_count:]
            rows.append(primary_table)

        if one:
            rows = rows[0] if rows else None

        return rows


def get_class(item):
    """get class of model or QueryTable"""
    if isinstance(item, QueryTable):
        value = item.cls
    else:
        value = inspect.getmro(item)[0]
    return value


class QueryTable:  # pylint: disable=too-many-instance-attributes
    """container for a query table"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 cls, alias=None, column=None, join_type=None,
                 join_table_name=None, join_column_name=None):

        self.cls = cls
        self.alias = alias or snake_to_camel(cls.__name__)
        self.table_name = alias or cls.__name__
        self.column_count = len(cls._m.fields)

        self.join_type = join_type
        self.join_column = column
        self.join_table_name = join_table_name
        self.join_table_column = join_column_name

    def join(self, quote):
        """return join clause"""
        if self.join_type is None:
            return '{Q}{table}{Q} AS {Q}{alias}{Q}'.format(
                Q=quote, table=self.name, alias=self.alias
            )

        join = self.join_type

        join += ' {Q}{table}{Q} AS {Q}{alias}{Q}'.format(
            Q=quote, table=self.name, alias=self.alias
        )

        join += ' ON {Q}{table}{Q}.{Q}{column}{Q}'.format(
            Q=quote, table=self.alias, column=self.join_column
        )

        return join + ' = {Q}{table}{Q}.{Q}{column}{Q}'.format(
            Q=quote, table=self.join_table_name, column=self.join_table_column
        )

    @property
    def name(self):
        """return underlying table name"""
        return self.cls._m.table_name

    def _fields(self):
        return self.cls._m.db_read

    def _primary(self):
        return self.cls._m.primary

    @property
    def _m(self):
        return self.cls._m


def _column(table, field, quote):
    if field.expression:
        return field.expression.format(
            Q=quote, TABLE=quote + table.alias + quote)
    return '{Q}{table}{Q}.{Q}{column}{Q}'.format(
        Q=quote, table=table.alias, column=field.column
    )


def _pair(table, tables, table2=None):
    """Pair 'table' by foreign/primary key to a table in 'tables'

       table - a Model class
       tables - a list of QueryTables
       table2 - an alias, path or Model class matching one of 'tables'; this
                limits pairing to a single table

       return:
           tuple of:
               matching column name in table
               matching QueryTable alias
               matching column name in QueryTable
    """

    if table2:
        match = [t for t in tables if t.alias == table2]
        if match:
            tables = match
        else:
            try:
                table2 = import_by_path(table2)
            except ValueError as exc:
                raise TypeError(f"invalid path to table: '{table2}'") from exc
            match = [t for t in tables if t.cls == table2]
            if not match:
                raise TypeError(
                    "'{}' does not match any tables".format(
                        table2.__name__
                    )
                )
            if len(match) > 1:
                raise TypeError(
                    "'{}' matches multiple tables".format(table2.__name__)
                )
            tables = match

    ref = _find_foreign_key_reference(table, tables)
    if ref:
        tab, field = ref
        return field, tab.alias, tab._primary().name

    ref = _find_primary_key_reference(table, tables)
    if ref:
        tab, field = ref
        return table._m.primary.name, tab.alias, field

    raise TypeError(
        "no primary or foreign key matches found for '{}'".format(
            table.__name__
        )
    )


def _find_foreign_key_reference(table, tables):
    """Look for a foreign key reference in 'table' to one of 'tables'

       table - a Model class
       tables - a list or tuple of Models or QueryTables
    """
    try:
        foreign = table._m.foreign
    except AttributeError as exc:
        raise TypeError('table must be a Model or QueryTable') from exc
    if len(foreign) == 0:
        return None
    refs = [
        (t, f.name) for f in foreign
        for t in tables
        if f.foreign == get_class(t)
    ]
    if len(refs) == 0:
        return None
    if len(refs) > 1:
        raise TypeError(
            "'{}' has multiple foreign keys that match".format(
                table.__name__
            )
        )
    return refs[0]


def _find_primary_key_reference(table, tables):
    """Look for a primary key reference in 'table' from one of 'tables'

       table - a Model class
       tables - a list or tuple of Models or QueryTables
    """
    refs = [
        (t, f.name) for t in tables
        for f in t._m.foreign
        if f.foreign == get_class(table)
    ]
    if len(refs) == 0:
        return None
    if len(refs) > 1:
        raise TypeError(
            "'{}' matches mutiple foreign keys".format(
                table.__name__
            )
        )
    return refs[0]
