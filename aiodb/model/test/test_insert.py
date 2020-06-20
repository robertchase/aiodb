"""test model insert method"""
from aiodb.model.test.test_save import MockTable


def test_insert(cursor, run):
    """verify insert with specified primary key"""

    primary_key = 100
    test = MockTable(name='test')
    run(test.insert, cursor, primary_key)
    assert test.the_key == primary_key
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,100 )"


def test_insert_pk(cursor, run):
    """verify insert with instance primary key"""

    test = MockTable(the_key=1000, name='test')
    run(test.insert, cursor)
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'name','the_key' ) VALUES ( test,1000 )"
