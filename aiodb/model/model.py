"""Object Relational Model"""
# pylint: disable=protected-access
from aiodb.cursor import Raw
from aiodb.model.field import Field
from aiodb.model.query import Query
from aiodb.util import snake_to_camel


__reserved__ = ("load", "save", "delete", "query")


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


def get_tablename(model):
    """return the model's table name"""
    return model._m.table_name


def get_updated(model):
    """return dict of updated fields for model

       {"name" : (old_value, new_value), ...}
    """
    return model._s.updated


def as_dict(model):
    """return the model field+values as a dict"""
    return {
        fld.name: getattr(model, fld.name)
        for fld in model._m.fields
    }


class _ModelState:  # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-instance-attributes

    def __init__(self, table_name=None):
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
    """metaclass for base orm

       The stuff that happens at Model class instantiation

           1. determine table name
           2. digest the Fields
    """

    def __new__(cls, name, supers, attrs):

        # --- table name
        if "TABLENAME" in attrs:
            table_name = attrs["TABLENAME"]
            del attrs["TABLENAME"]
        else:
            table_name = snake_to_camel(name)

        # --- create the "_m" attribute to hold shared Model state
        state = attrs["_m"] = _ModelState(table_name=table_name)

        # --- grab fields from supers and class
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

        for sup in supers[::-1]:
            update_fields(sup.__dict__)
        update_fields(attrs)

        # --- add fields to state
        state.fields = fields
        state.db_read = [fld for fld in fields if fld.is_database]
        state.db_insert = [fld for fld in state.db_read if not fld.is_readonly]
        state.db_update = [
            fld for fld in state.db_insert if not fld.is_primary]

        primary = [fld for fld in fields if fld.is_primary]
        if len(primary) > 1:
            raise MultiplePrimaryKeysError()
        if len(primary) == 1:
            state.primary = primary[0]

        state.foreign = [fld for fld in state.db_read if fld.is_foreign]

        return super().__new__(cls, name, supers, attrs)

    @property
    def query(cls):  # a property on the metaclass is a "classproperty"
        """return query object for class"""
        return Query(cls)


class _State:  # pylint: disable=too-few-public-methods
    """instance state"""

    def __init__(self):
        self.values = {}  # instance value store
        self.original = {}  # cache of field values from init or save
        self.updated = {}  # list of changes processed at most recent save
        self.tables = {}  # dict of joined models from query


class Model(metaclass=_Model):
    """base orm

       Notes:
           1. The table name is derived by converting the class name from
              snake to camel case.  For instance, "MySpecialTable" becomes
              "my_special_table". The TABLENAME class attribute will override
              this behavior by directly specifiying the table name.

           2. The only non "_*" attributes in the Model namespace are the
              methods: "load", "save" and "delete", and the property "query".
              Field names cannot be assigned to these values, and method names
              which override these values must take the appropriate care in
              order to maintain core functionality.

           3. The Model's State is kept in the "_m" attribute and the instance
              state is kept in the "_s" attribute.  The "_m" attribute is
              shared with all instances.
    """

    def __init__(self, **kwargs):
        self._s = _State()

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
        key = self._m.primary
        if key:
            val = getattr(self, key.name)
        else:
            val = None

        result = f"{self.__class__.__name__}("
        if val:
            result += f"primary_key={val}"
        else:
            result += f"object_id={id(self)}"

        result += ")"
        return result

    def __getitem__(self, name):
        return self._s.tables[name]

    def __getattribute__(self, name):
        if name.startswith("_"):
            value = object.__getattribute__(self, name)
        else:
            try:
                value = object.__getattribute__(self, name)
            except AttributeError:
                try:
                    # dot notation access for joined tables
                    return self._s.tables[name]
                except (AttributeError, KeyError):
                    pass
                raise

            if isinstance(value, Field):
                values = self._s.values
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
            values = self._s.values
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
        query = cls.query.where(f'{quote(cls._m.primary.name)}=%s')
        return await query.execute(cursor, key, one=True)

    async def save(self, cursor, force_insert=False):
        """Insert or update database with values in model

           If there is no primary key, then an INSERT is performed.

           If the primary key has a value, an UPDATE is performed; otherwise,
           an INSERT is performed and the auto-generated primary key value is
           added to the object.

           To force INSERT even when the primary key has a value, set
           'force_insert' to True.

           Returns self.

           Notes:
               1. On INSERT the "_s.updated" attribute is set to
                  {field_name: (None, value), ...} for each inserted field.

               1. On UPDATE, only changed fields -- if any -- are SET. The
                  "_s.updated" attribute is set to
                  {field_name: (old_value, new_value), ...} for each changed
                  field.

               2. This call will not change expression fields in the Model
                  instance.
        """
        key = self._m.primary
        self._s.updated = {}
        cursor.query = None
        cursor.query_after = None
        stmt = ''

        if force_insert:
            if not key:
                raise Exception("force_insert is not valid without primary key")
            if getattr(self, key.name) is None:
                raise Exception(
                    "force_insert is not valid without a primary key value")
            is_insert = True
        elif key is None:
            is_insert = True
        elif getattr(self, key.name) is None:
            is_insert = True
        else:
            is_insert = False

        if is_insert:
            fields = self._m.db_insert if force_insert else self._m.db_update
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
            fields = fields_to_update(self)
            if fields:
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

        if fields:
            stmt = stmt.format(Q=cursor.quote)
            await cursor.execute(stmt, args,
                                 is_insert=is_insert,
                                 pk=key.name if key else None)

            # grab database-assigned primary key
            if is_insert:
                if not force_insert:
                    if key:
                        setattr(self, key.name, cursor.last_id())

            self._s.updated = {
                fld.name: (
                    None if is_insert else self._s.original.get(fld.name),
                    getattr(self, fld.name))
                for fld in fields}
            cache_field_values(self)

        return self

    async def delete(self, cursor):
        """Delete matching row from database by primary key"""
        stmt = (
            f"DELETE FROM {quote(self._m.table_name)}"
            f" WHERE {quote(self._m.primary.name)}=%s"
        ).format(Q=cursor.quote)
        await cursor.execute(stmt, getattr(self, self._m.primary.name))


def cache_field_values(model):
    """cache model field values in '_s.original'"""
    model._s.original = {
        fld.name: getattr(model, fld.name) for
        fld in model._m.db_update
    }


def fields_to_update(model):
    """return list of changed fields or None"""
    fields = [
        fld
        for fld in model._m.db_update
        if getattr(model, fld.name) != model._s.original[fld.name]
    ]
    return None if len(fields) == 0 else fields
