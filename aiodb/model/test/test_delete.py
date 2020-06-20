"""verify delete operation"""
from aiodb.model.test.test_save import MockTable


def test_delete(cursor, run):
    """verify delete operation"""

    test = MockTable(the_key=100, name='foo')

    run(test.delete, cursor)
    assert cursor.query == \
        "DELETE FROM 'tester' WHERE 'the_key'=%s"
    assert cursor.query_after == \
        "DELETE FROM 'tester' WHERE 'the_key'=100"
