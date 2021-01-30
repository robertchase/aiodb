"""test model operation"""
# pylint: disable=protected-access
import pytest
from aiodb import Model, Field
from aiodb import ReservedAttributeError, RequiredAttributeError
from aiodb import NoneValueError, MultiplePrimaryKeysError


def test_table_name():
    """test table naming"""

    class MyTestTable1(Model):
        """test model"""
    assert MyTestTable1._m.table_name == "my_test_table1"

    class MyTestTable2(Model):
        """test model"""
        TABLENAME = "foo_bar"
    assert MyTestTable2._m.table_name == "foo_bar"


def test_reserved():
    """verify reserved operation"""

    with pytest.raises(ReservedAttributeError):
        class Test(Model):  # pylint: disable=unused-variable
            """model with a field name violating reserved words"""
            save = Field()


def test_name():
    """verify column field setting"""

    class Test(Model):
        """model with a field using column"""
        yeah = Field(column='select', is_nullable=True)

    test = Test()
    assert test._m.field('yeah').name == 'yeah'
    assert test._m.field('yeah').column == 'select'


def test_required():
    """verify required field setting"""

    class Test(Model):
        """model with one required field"""
        test = Field()

    with pytest.raises(RequiredAttributeError):
        Test()

    Test(test='foo')


def test_default():
    """verify default field setting"""

    default = 'abc'

    class Test(Model):
        """model with default field"""
        test = Field(default=default)

    test = Test()
    assert test.test == default
    test = Test(test=default * 2)
    assert test.test != default


def test_is_nullable():
    """verify enforcement of nullable fields"""

    class Test(Model):
        """model with nullable and not-nullable fields"""
        test = Field()
        nullable = Field(is_nullable=True)

    Test(test='something')
    Test(test='somethng', nullable='nothing')
    with pytest.raises(NoneValueError):
        Test(test=None)


def test_multiple_primary():
    """test prevention of multiple primary keys"""

    with pytest.raises(MultiplePrimaryKeysError):
        class Test(Model):  # pylint: disable=unused-variable
            """bad model with multiple primary keys"""
            id = Field(default=0, is_primary=True)
            idd = Field(default=0, is_primary=True)


class FieldTest(Model):
    """helper class for verify tests"""
    a = Field(is_primary=True)
    b = Field()
    c = Field(expression='foo')
    d = Field(is_database=False)


def test_field():
    """verify _field"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert test._m.field('a').name == 'a'


def test_fields():
    """verify _fields"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in test._m.fields) == ['a', 'b', 'c', 'd']


def test_primary():
    """verify _primary"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert test._m.primary.name == 'a'


def test_db_read():
    """verify _db_read"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in test._m.db_read) == ['a', 'b', 'c']


def test_db_insert():
    """verify _db_insert"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in test._m.db_insert) == ['a', 'b']


def test_db_update():
    """verify _db_update"""
    test = FieldTest(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in test._m.db_update) == ['b']


def test_repr():
    """test __repr__ operation"""

    class Test(Model):
        """simple model with a primary key"""
        id = Field(is_primary=True)

    assert str(Test(id=10)) == 'Test(primary_key=10)'


class JoinedModel(Model):
    """fake model that looks like it as a joined table"""

    def __init__(self):
        super().__init__()
        self._s.tables = {'a': 'one'}


def test_join_lookup_bracket():
    """find joined table using bracket notation"""

    model = JoinedModel()
    assert model['a'] == 'one'
    with pytest.raises(KeyError):
        assert model['b'] == 'one'


def test_join_lookup_dot():
    """find joined table using dot notation"""

    model = JoinedModel()
    assert model.a == 'one'
    with pytest.raises(AttributeError):
        assert model.b == 'one'
