"""test model insert method"""
from tests.test_save import MockTable


def test_insert(cursor, run):
    """verify insert with specified primary key"""

    primary_key = 100
    test = MockTable(name='test', the_key=primary_key)
    run(test.save, cursor, True)
    assert test.the_key == primary_key
    assert cursor.query_after == \
        "INSERT INTO 'tester' ( 'the_key','name' ) VALUES ( 100,test )"
