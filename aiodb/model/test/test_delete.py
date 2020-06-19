from aiodb.model.test.test_save import MockTable


def test_delete(cursor, run):

    o = MockTable(the_key=100, name='foo')

    run(o.delete, cursor)
    assert cursor.query == \
        "DELETE FROM 'tester' WHERE 'the_key'=%s"
    assert cursor.query_after == \
        "DELETE FROM 'tester' WHERE 'the_key'=100"
