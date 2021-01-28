"""test load operation"""
from aiodb import Model, Field


class MyTable(Model):
    """test model"""
    id = Field(is_primary=True)
    name = Field()
    email = Field()


def test_load(cursor, run):
    """verify simple load by key"""
    _ = run(MyTable.load, cursor, 123)
    assert cursor.query_after == (
        "SELECT 'my_table'.'id' AS 0_id, 'my_table'.'name' AS 0_name,"
        " 'my_table'.'email' AS 0_email FROM 'my_table' AS 'my_table'"
        " WHERE 'id'=123 LIMIT 1"
    )
