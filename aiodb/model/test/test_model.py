import pytest
from aiodb import Model, Field
from aiodb import ReservedAttributeError, RequiredAttributeError
from aiodb import NoneValueError, MultiplePrimaryKeysError


def test_reserved():

    class test(Model):
        save = Field()

    with pytest.raises(ReservedAttributeError):
        test()


def test_name():

    class test(Model):
        foo = Field(column='select', is_nullable=True)

    t = test()
    assert t._field('foo').name == 'foo'
    assert t._field('foo').column == 'select'


def test_required():

    class test(Model):
        test = Field()
        test_read = Field(is_readonly=True)

    with pytest.raises(RequiredAttributeError):
        test()

    test(test='foo')


def test_default():

    default = 'abc'

    class test(Model):
        test = Field(default=default)

    t = test()
    assert t.test == default
    t = test(test=default * 2)
    assert t.test != default


def test_is_nullable():

    class test(Model):
        test = Field()
        nullable = Field(is_nullable=True)

    test(test='something')
    test(test='somethng', nullable='nothing')
    with pytest.raises(NoneValueError):
        test(test=None)


def test_multiple_primary():

    class test(Model):
        id = Field(default=0, is_primary=True)
        idd = Field(default=0, is_primary=True)

    t = test()
    with pytest.raises(MultiplePrimaryKeysError):
        assert t._primary.name == 'id'


class field_test(Model):
    a = Field(is_primary=True)
    b = Field()
    c = Field(expression='foo')
    d = Field(is_database=False)


def test_field():
    t = field_test(a=0, b=0, c=0, d=0)
    assert t._field('a').name == 'a'


def test_fields():
    t = field_test(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in t._fields) == ['a', 'b', 'c', 'd']


def test_primary():
    t = field_test(a=0, b=0, c=0, d=0)
    assert t._primary.name == 'a'


def test_db_read():
    t = field_test(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in t._db_read) == ['a', 'b', 'c']


def test_db_insert():
    t = field_test(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in t._db_insert) == ['a', 'b']


def test_db_update():
    t = field_test(a=0, b=0, c=0, d=0)
    assert sorted(f.name for f in t._db_update) == ['b']
    assert sorted(f.name for f in t._db_update) == ['b']
