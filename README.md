# aiodb
## async/await enabled database access

`aiodb` is a general `cursor` and `orm` which
asynchronously interact with relational databases
using python's `async`/`await` features.

## An Example

Here is a program that defines a `model` for a table named `test`, and then
saves a new row in the table.

The cursor is bound to a database-specific connection for `mysql`
imported from aiomysql.

```
from aiodb import Cursor, Model, Field, Integer
from aiomysql import MysqlConnector


class Test(Model):
    __TABLENAME__ = 'test'
   id = Field(Integer, is_primary=True)
   name = Field()


async def main():
    con = await MysqlConnector(host="db", user="fred", password="flintstone")
    cursor = Cursor.bind(con)
    t = Test(name='barney')
    await t.save(cursor)
    print(t.as_dict())


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
```

The two places where the program might block for I/O
are at `MysqlConnector` construction (where the database connection is established)
and at `t.save(cursor)` (where the INSERT happens);
both places use `await` to allow other things to occur while the I/O happens.

Here is a sample run. The output is a dictionary representation of the saved `Test` object.
The `id` value is auto-generated in the database and stored in the object.

```
# python my_program.py
{'id': 123, 'name': 'barney'}
```
