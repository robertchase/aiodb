from aiodb import Model, Field, Integer


def test_connect(db_defn_sync):
    cursor = db_defn_sync.cursor()
    assert cursor
    cursor.close()


class Table(Model):
    __TABLENAME__ = 'test'
    id = Field(Integer, is_primary=True)
    data = Field(column='g_vch')


def test_insert(cursor_sync):
    test = Table(data='my data')
    test.save(cursor_sync)
    assert test.id


def test_update(cursor_sync):
    test = Table(id=10, data='my data')
    test.data = 'akk'
    test.save(cursor_sync)
    assert test._updated == ['data']


def test_load(cursor_sync):
    test = Table(data='my data')
    test.save(cursor_sync)
    test_load = Table.load_sync(cursor_sync, test.id)
    assert test.data == test_load.data


def test_delete(cursor_sync):
    test = Table(data='my data')
    test.save(cursor_sync)
    test.delete(cursor_sync)
    test_load = Table.load_sync(cursor_sync, test.id)
    assert test_load is None
