"""Object Relational Model"""
# pylint: disable=protected-access
from aiodb.cursor import Raw
from aiodb.model.field import Field
from aiodb.model.query import Query
from aiodb.util import snake_to_camel


__reserved__ = ('as_dict', 'delete', 'load', 'save')


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


class _Meta:  # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-instance-attributes

    def __init__(self, table_name=None):
        self.cls = None
        self.table_name = table_name
        self.fields = None
        self.db_read = None
        self.db_insert = None
        self.db_update = None
        self.primary = None
        self.foreign = None

    def field(self, name):
        """return a field by name"""
        try:
            return [field for field in self.fields if field.name == name][0]
        except IndexError as exc:
            raise AttributeError(name) from exc


class _Model(type):
    """metaclass for base orm"""

    def __new__(cls, name, supers, attrs):

        if "Meta" in attrs:
            user_meta = attrs["Meta"].__dict__
            del attrs["Meta"]
        else:
            user_meta = {}

        meta = attrs["_m"] = _Meta(**user_meta)

        if meta.table_name is None:
            meta.table_name = snake_to_camel(name)

        fields = []

        def update_fields(data):
            for key, value in data.items():
                if isinstance(value, Field):
                    if key in __reserved__:
                        raise ReservedAttributeError(key)
                    if value.column is None:
                        value.column = key
                    value.name = key
                    fields.append(value)

        update_fields(attrs)
        for sup in supers:
            update_fields(sup.__dict__)

        meta.fields = fields
        meta.db_read = [fld for fld in fields if fld.is_database]
        meta.db_insert = [fld for fld in meta.db_read if not fld.is_readonly]
        meta.db_update = [fld for fld in meta.db_insert if not fld.is_primary]

        primary = [fld for fld in fields if fld.is_primary]
        if len(primary) > 1:
            raise MultiplePrimaryKeysError()
        if len(primary) == 1:
            meta.primary = primary[0]

        meta.foreign = [fld for fld in meta.db_read if fld.is_foreign]

        return super().__new__(cls, name, supers, attrs)

    @property
    def query(cls):
        """return query object for class"""
        return Query(cls)


class Model(metaclass=_Model):
    """base orm"""

    def __init__(self, **kwargs):
        self._orig = {}
        self._updated = {}
        self._values = {}

        for field in self._m.fields:
            if not field.is_nullable and field.default is None:
                if field.name not in kwargs:
                    raise RequiredAttributeError(field.name)
            else:
                setattr(self, field.name, field.default)
        for name, value in kwargs.items():
            setattr(self, name, value)

        cache_field_values(self)

    def __repr__(self):
        result = f'{self.__class__.__name__}('
        if self._m.primary and (val := getattr(self, self._m.primary.name)):
            result += f'primary_key={val}'
        else:
            result += f'object_id={id(self)}'

        result += ')'
        return result

    def as_dict(self):
        """return instance as dict"""
        return {
            fld.name: getattr(self, fld.name)
            for fld in self._m.fields
        }

    def __getitem__(self, name):
        return self._tables[name]

    def __getattribute__(self, name):
        try:
            value = object.__getattribute__(self, name)
        except AttributeError:
            try:
                # dot notation access for joined tables
                return object.__getattribute__(self, '_tables')[name]
            except (AttributeError, KeyError):
                pass
            raise

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
    async def load(cls, cursor, key):
        """Load a database row by primary key"""
        query = Query(cls).where(f'{quote(cls._m.primary.name)}=%s')
        return await query.execute(cursor, key, one=True)

    async def save(self, cursor, insert=False):
        """Save object by primary key

           If the primary key has a value, an UPDATE is performed; otherwise,
           an INSERT is performed and the auto-generated primary key value is
           added to the object.

           To force INSERT even if the primary key has a value, set 'insert'
           to True.

           Returns self.

           Notes:

               1. On UPDATE, only changed fields, if any, are SET. A dict of
                  {field_name: (old_value, new_value), ...} is found in the
                  '_updated' attribute.

               2. This call will not change expression Fields.
        """
        key = self._m.primary
        self._updated = {}
        cursor.query = None
        cursor.query_after = None
        stmt = ''
        if insert or key is None or getattr(self, key.name) is None:
            new = True
            fields = self._m.db_insert if insert else self._m.db_update
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
                quote(self._m.table_name),
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
            fields = fields_to_update(self)
            if fields is not None:
                stmt = ' '.join((
                    'UPDATE ',
                    quote(self._m.table_name),
                    'SET',
                    ','.join([f'{quote(fld.column)}=%s' for fld in fields]),
                    'WHERE ',
                    f'{quote(key.name)}=%s'
                ))
                args = [getattr(self, fld.name) for fld in fields]
                args.append(getattr(self, key.name))

        stmt = stmt.format(Q=cursor.quote)

        if fields:
            await cursor.execute(stmt, args, is_insert=new, pk=key.name)
            if new:
                if not insert and key:
                    setattr(self, key.name, cursor.last_id())
            else:
                self._updated = {
                    fld.name: (self._orig[fld.name], getattr(self, fld.name))
                    for fld in fields
                }
            cache_field_values(self)

        return self

    async def insert(self, cursor, key=None):
        """Insert object

           Insert usually happens automatically when key is NOT specified; this
           is for the case where you want to specifiy the primary key yourself.

           If 'key' is specified, then the primary key value is set to this
           value; otherwise, the current value of the primary key is used.

           Returns self.
        """
        if key is not None:
            setattr(self, self._m.primary.name, key)
        return await self.save(cursor, insert=True)

    async def delete(self, cursor):
        """Delete matching row from database by primary key.
        """
        stmt = (
            f"DELETE FROM {quote(self._m.table_name)}"
            f" WHERE {quote(self._m.primary.name)}=%s"
        ).format(Q=cursor.quote)
        await cursor.execute(stmt, getattr(self, self._m.primary.name))


def cache_field_values(model):
    """cache model field values in '_orig'"""
    model._orig = {
        fld.name: getattr(model, fld.name) for
        fld in model._m.db_update
    }


def fields_to_update(model):
    """return list of changed fields or None"""
    fields = [
        fld
        for fld in model._m.db_update
        if getattr(model, fld.name) != model._orig[fld.name]
    ]
    if len(fields) == 0:
        return None
    return fields
