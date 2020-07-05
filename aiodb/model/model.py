"""Object Relational Model"""
from collections import namedtuple

from aiodb.model.cursor import Raw
from aiodb.model.field import Field
from aiodb.model.query import Query


__reserved__ = ('as_dict', 'delete', 'load', 'load_sync', 'save')


class RequiredAttributeError(AttributeError):
    """custom exception"""


class ReservedAttributeError(AttributeError):
    """custom exception"""


class NoneValueError(ValueError):
    """custom exception"""


class MultiplePrimaryKeysError(AttributeError):
    """custom exception"""


def quote(name):
    """return value surrounded with substitutable quote marks"""
    return '{Q}' + name + '{Q}'


PreSave = namedtuple('PreSave', 'stmt, args, new, key, fields', defaults=(
    None, None, False, None, None))


class Model:
    """base orm"""

    __TABLENAME__ = None

    def __init__(self, **kwargs):
        self._values = {}
        for field in self._fields():
            if not field.is_nullable and field.default is None:
                if field.name not in kwargs:
                    raise RequiredAttributeError(field.name)
            else:
                setattr(self, field.name, field.default)
        for name, value in kwargs.items():
            setattr(self, name, value)
        self._cache_field_values()

    def __repr__(self):
        result = f'{self._class().__name__}('
        if self._primary() and (val := getattr(self, self._primary().name)):
            result += f'primary_key={val}'
        else:
            result += f'object_id={id(self)}'

        result += ')'
        return result

    def as_dict(self):
        """return instance as dict"""
        return {
            fld.name: getattr(self, fld.name)
            for fld in self._fields()
        }

    @classmethod
    def _fields(cls):
        if hasattr(cls, '_field_cache'):
            return cls._field_cache
        fields = []
        for name in dir(cls):
            if name.startswith('_'):
                continue
            if name == 'query':
                continue
            value = getattr(cls, name)
            if isinstance(value, Field):
                if name in __reserved__:
                    raise ReservedAttributeError(name)
                if value.column is None:
                    value.column = name
                value.name = name
                fields.append(value)
        cls._field_cache = fields
        return fields

    @classmethod
    def _field(cls, name):
        try:
            return [field for field in cls._fields() if field.name == name][0]
        except IndexError:
            raise AttributeError(name)

    @classmethod
    def _db_read(cls):
        return [fld for fld in cls._fields() if fld.is_database]

    @classmethod
    def _db_insert(cls):
        return [fld for fld in cls._db_read() if not fld.is_readonly]

    @classmethod
    def _db_update(cls):
        return [fld for fld in cls._db_insert() if not fld.is_primary]

    @classmethod
    def _primary(cls):
        primary = [fld for fld in cls._fields() if fld.is_primary]
        if len(primary) > 1:
            raise MultiplePrimaryKeysError()
        if len(primary) == 1:
            return primary[0]
        return None

    @classmethod
    def _foreign(cls):
        return [fld for fld in cls._db_read() if fld.is_foreign]

    @classmethod
    def _class(cls):
        return cls

    def __getitem__(self, name):
        return self._tables[name]

    def __getattribute__(self, name):
        try:
            value = object.__getattribute__(self, name)
        except AttributeError:
            try:
                # dot notation access for joined tables
                return object.__getattribute__(self, '_tables')[name]
            except AttributeError:
                pass
            except KeyError:
                pass
            raise
        if name in ('_primary',):
            return value
        if isinstance(value, Field):
            values = object.__getattribute__(self, '_values')
            if name not in values:
                raise AttributeError(name)
            value = values[name]
        return value

    def __setattr__(self, name, value):
        if name.startswith('_'):
            object.__setattr__(self, name, value)
        else:
            attr = object.__getattribute__(self, name)
            if not isinstance(attr, Field):
                raise AttributeError(name)
            values = object.__getattribute__(self, '_values')
            if value is None:
                if not attr.is_nullable:
                    raise NoneValueError(name)
                values[name] = value
            elif isinstance(value, Raw):
                values[name] = value
            else:
                values[name] = attr.parse(value)

    @classmethod
    def query(cls):
        """start query on model"""
        return Query(cls)

    @classmethod
    def _pre_load(cls):
        return Query(cls).where(f'{quote(cls._primary().name)}=%s')

    @classmethod
    async def load(cls, cursor, key):
        """Load a database row by primary key.
        """
        query = cls._pre_load()
        return await query.execute(cursor, key, one=True)

    @classmethod
    def load_sync(cls, cursor, key):
        """Load a database row by primary key (sync mode).
        """
        query = cls._pre_load()
        return query.execute(cursor, key, one=True)

    def _pre_save(self, cursor, insert=False):
        key = self._primary()
        self._updated = []  # pylint: disable=attribute-defined-outside-init
        cursor.last_id = None
        cursor.query = None
        cursor.query_after = None
        if insert or key is None or getattr(self, key.name) is None:
            new = True
            fields = self._db_insert() if insert else self._db_update()
            fields = [
                f
                for f in fields
                if not (f.is_nullable and getattr(self, f.name) is None)
            ]
            field_names = [
                f.column
                for f in fields
            ]
            stmt = ' '.join((
                'INSERT INTO',
                quote(self.__TABLENAME__),
                '(',
                ','.join(quote(f) for f in field_names),
                ') VALUES (',
                ','.join('%s' for n in range(len(field_names))),
                ')',
            ))
            args = [getattr(self, f.name) for f in fields]
        else:
            if not getattr(self, key.name):
                raise Exception('Model UPDATE requires a primary key')
            new = False
            fields = self._fields_to_update
            if fields is None:
                return PreSave()
            stmt = ' '.join((
                'UPDATE ',
                quote(self.__TABLENAME__),
                'SET',
                ','.join([f'{quote(fld.column)}=%s' for fld in fields]),
                'WHERE ',
                f'{quote(key.name)}=%s'
            ))
            args = [getattr(self, fld.name) for fld in fields]
            args.append(getattr(self, key.name))

        stmt = stmt.format(Q=cursor.quote)
        return PreSave(stmt, args, new, key, fields)

    def _post_save(self, cursor, insert, pre_result):
        self._cache_field_values()
        if pre_result.new:
            if not insert and pre_result.key:
                setattr(self, pre_result.key.name, cursor.last_id)
        else:
            self._updated = [  # pylint: disable=attribute-defined-outside-init
                field.name for field in pre_result.fields]

        return self

    async def save(self, cursor, insert=False):
        """Save object by primary key

           If the primary key has a value, an UPDATE is performed; otherwise,
           an INSERT is performed and the auto-generated primary key value is
           added to the object.

           To force INSERT even if the primary key has a value, set 'insert'
           to True.

           Returns self.

           Notes:

               1. On UPDATE, only changed fields, if any, are SET. A list of
                  update field names is found in the '_updated' attribute.

               2. This call will not change expression Fields.
        """
        result = self._pre_save(cursor, insert)
        if result.fields:
            await cursor.execute(result.stmt, result.args,
                                 is_insert=result.new, pk=result.key.name)
            self._post_save(cursor, insert, result)
        return self

    def _pre_insert(self, key):
        if key is not None:
            setattr(self, self._primary().name, key)

    async def insert(self, cursor, key=None):
        """Insert object

           Insert usually happens automatically when key is NOT specified; this
           is for the case where you want to specifiy the primary key yourself.

           If 'key' is specified, then the primary key value is set to this
           value; otherwise, the current value of the primary key is used.

           Returns self.
        """
        self._pre_insert(key)
        return await self.save(cursor, insert=True)

    def _pre_delete(self, cursor):
        stmt = f'DELETE FROM {quote(self.__TABLENAME__)}' + \
            f' WHERE {quote(self._primary().name)}=%s'
        return stmt.format(Q=cursor.quote)

    async def delete(self, cursor):
        """Delete matching row from database by primary key.
        """
        stmt = self._pre_delete(cursor)
        await cursor.execute(stmt, getattr(self, self._primary().name))

    def _cache_field_values(self):
        self._orig = {
            fld.name: getattr(self, fld.name) for
            fld in self._db_update()
        }

    @property
    def _fields_to_update(self):
        fields = [
            fld
            for fld in self._db_update()
            if getattr(self, fld.name) != self._orig[fld.name]
        ]
        if len(fields) == 0:
            return None
        return fields

    @classmethod
    def _patch(cls):
        """replace all async methods in Model class"""
        cls.save = cls._save_sync
        cls.insert = cls._insert_sync
        cls.delete = cls._delete_sync

    def _save_sync(self, cursor, insert=False):
        """Save object by primary key
        """
        result = self._pre_save(cursor, insert)
        if result.fields:
            cursor.execute(result.stmt, result.args,
                           is_insert=result.new, pk=result.key.name)
            self._post_save(cursor, insert, result)
        return self

    def _insert_sync(self, cursor, key=None):
        """Insert object
        """
        self._pre_insert(key)
        return self.save(cursor, insert=True)

    def _delete_sync(self, cursor):
        """Delete matching row from database by primary key.
        """
        stmt = self._pre_delete(cursor)
        cursor.execute(stmt, getattr(self, self._primary().name))
