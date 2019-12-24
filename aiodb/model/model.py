from aiodb.model.field import Field
from aiodb.model.query import Query


__reserved__ = ('query', 'select')


class RequiredAttributeError(AttributeError):
    pass


class ReservedAttributeError(AttributeError):
    pass


class NoneValueError(ValueError):
    pass


class MultiplePrimaryKeysError(AttributeError):
    pass


def quote(name):
    return '{Q}' + name + '{Q}'


class Model:

    __TABLENAME__ = None

    def __init__(self, **kwargs):
        self._values = {}
        for field in self._fields:
            if not field.is_nullable and field.default is None:
                if field.name not in kwargs:
                    raise RequiredAttributeError(field.name)
            else:
                setattr(self, field.name, field.default)
        for name, value in kwargs.items():
            setattr(self, name, value)
        self._cache_field_values()

    def __repr__(self):
        result = f'{self._class.__name__}('
        if self._primary and getattr(self, self._primary.name):
            val = getattr(self, self._primary.name)
            result += f'primary_key={val}'
        else:
            result += f'object_id={id(self)}'
        result += ')'
        return result

    def as_dict(self):
        return {
            fld.name: getattr(self, fld.name)
            for fld in self._fields
        }

    class _classproperty(object):
        """Hack for property-like access to class method

           https://stackoverflow.com/questions/5189699/how-to-make-a-class-property
        """
        def __init__(self, fn):
            self.fn = fn

        def __get__(self, theinstance, theclass):
            return self.fn(theclass)

    @_classproperty
    def _fields(cls):
        fields = []
        for name in dir(cls):
            if name.startswith('_'):
                continue
            value = getattr(cls, name)
            if isinstance(value, Field):
                if name in __reserved__:
                    raise ReservedAttributeError(name)
                if value.column is None:
                    value.column = name
                value.name = name
                fields.append(value)
        return fields

    @classmethod
    def _field(cls, name):
        try:
            return [field for field in cls._fields if field.name == name][0]
        except IndexError:
            raise AttributeError(name)

    @_classproperty
    def _db_read(cls):
        return [fld for fld in cls._fields if fld.is_database]

    @_classproperty
    def _db_insert(cls):
        return [fld for fld in cls._db_read if not fld.is_readonly]

    @_classproperty
    def _db_update(cls):
        return [fld for fld in cls._db_insert if not fld.is_primary]

    @_classproperty
    def _primary(cls):
        primary = [fld for fld in cls._fields if fld.is_primary]
        if len(primary) > 1:
            raise MultiplePrimaryKeysError()
        if len(primary) == 1:
            return primary[0]
        return None

    @_classproperty
    def _foreign(cls):
        return [fld for fld in cls._db_read if fld.is_foreign]

    @_classproperty
    def _class(cls):
        return cls

    @classmethod
    def _alias(cls):
        camel = cls.__name__
        return ''.join(
            [
                c if c.islower() else '_' + c.lower()
                for c in camel[0].lower() + camel[1:]
            ]
        )

    def __getattribute__(self, name):
        value = object.__getattribute__(self, name)
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
            else:
                values[name] = attr.parse(value)

    @classmethod
    def query(cls):
        return Query(cls)

    @classmethod
    async def load(cls, cursor, id):
        q = Query(cls).where(f'{quote(cls._primary.name)}=%s')
        return await q.execute(cursor, id, one=True)

    async def save(self, cursor, insert=False):
        """Save database object by primary key

           Parameters:
               cursor - database cursor
               insert - bool
                        if True save object with non-None primary key using
                        INSERT instead of UPDATE

           Result:
               self

           Notes:

               1. Objects with a None primary key are INSERTED. After the
                  INSERT, the primary key attribute is set to the
                  auto-generated primary key.

               2. If the insert parameter is True, a new record is INSERTED,
                  and the primary key of the Modelis not changed.  The new
                  primary key is available as the cursor's last_id attribute.

               3. On UPDATE, only changed fields, if any, are SET.

               4. This call will not change expression Fields.
        """
        pk = self._primary
        self._updated = []
        cursor.last_id = None
        cursor.query = None
        cursor.query_after = None
        if insert or pk is None or getattr(self, pk.name) is None:
            new = True
            fields = self._db_insert if insert else self._db_update
            fields = [
                f.column
                for f in fields
                if not (f.is_nullable and getattr(self, f.name) is None)
            ]
            stmt = ' '.join((
                'INSERT INTO',
                quote(self.__TABLENAME__),
                '(',
                ','.join(quote(f) for f in fields),
                ') VALUES (',
                ','.join('%s' for n in range(len(fields))),
                ')',
            ))
            args = [getattr(self, f) for f in fields]
        else:
            if not getattr(self, pk.name):
                raise Exception('Model UPDATE requires a primary key')
            new = False
            fields = self._fields_to_update
            if fields is None:
                return self
            stmt = ' '.join((
                'UPDATE ',
                quote(self.__TABLENAME__),
                'SET',
                ','.join([f'{quote(fld.column)}=%s' for fld in fields]),
                'WHERE ',
                f'{quote(pk.name)}=%s'
            ))
            args = [getattr(self, fld.name) for fld in fields]
            args.append(getattr(self, pk.name))

        stmt = stmt.format(Q=cursor.quote)
        await cursor.execute(stmt, args)

        self._cache_field_values()
        if new:
            if not insert and pk:
                setattr(self, pk.name, cursor.last_id)
        else:
            self._updated = [field.name for field in fields]

        return self

    def _cache_field_values(self):
        self._orig = {
            fld.name: getattr(self, fld.name) for
            fld in self._db_update
        }

    @property
    def _fields_to_update(self):
        f = [
            fld
            for fld in self._db_update
            if getattr(self, fld.name) != self._orig[fld.name]
        ]
        if len(f) == 0:
            return None
        return f
