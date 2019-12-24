# aiodb
## async/await enabled database access

`aiodb` is an `orm`
and a set of connectors which
asynchronously interact with relational databases
using python's `async`/`await` features.

## An Example

Here is a program that defines a `model` for a table named `test`, and then
saves a new row in the table.

```
from aiodb import Model, Field, Integer
from aiodb.connector.mysql import DB


class Test(Model):
    __TABLENAME__ = 'test'
   id = Field(Integer, is_primary=True)
   name = Field()


async def main():
    db = DB(database='test', user='fred', password='flintstone')
    cursor = await db.cursor()
    t = Test(name='barney')
    await t.save(cursor)
    print(t.as_dict)


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
```

The two places where the program might block for I/O
are at `db.cursor()` (where the database connection is established)
and at `t.save(cursor)` (where the INSERT happens);
both places use `await` to allow other things to occur while the I/O happens.

Here is a sample run. The output is a dictionary representation of the saved `Test` object.
The `id` value is auto-generated in the database and stored in the object.

```
# python my_program.py
{'id': 123, 'name': 'barney'}
```
