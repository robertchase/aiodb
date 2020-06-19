import pytest

from aiodb.model.test.test_save import MockTable


def test_insert(cursor, run):

    ID = 100
    o = MockTable(name='test')
    run(o.insert, cursor, ID)
    assert o.the_key == ID
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,100 )"


def test_insert_pk(cursor, run):

    o = MockTable(the_key=1000, name='test')
    run(o.insert, cursor)
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,1000 )"
