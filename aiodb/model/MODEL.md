# Model
### aiodb model definition

## Introduction

A `Model` maps a database table to a python class.
What does this mean?
Simple operations like:
1. creating a new instance
2. saving a new or modified instance to the database
3. loading an instance from a database row

are easy to do with standard looking python syntax.
Even queries that involve joining tables by
primary and foreign keys are easy.

Complex queries are possible, but make more sense as
`SQL` rather than as python.
Why force python to simulate
transactions and set operations
when `SQL` is already
available and expressive? The `aiodb` framework
tries to maintain this balance.

## An Example

Here is a model that maps the `employee` table to a
python class named `Employee`:

```
from aiodb import Model, Field, Integer, Date, as_dict

class Employee(Model):
    id = Field(Integer, is_primary=True)
    name = Field()
    start_date = Field(Date)
```

#### create

The model maps three fields, `id`, `name` and `start_date`.
Here is how a new `Employee` is created:

```
>>> employee = Employee(name='Fred', start_date='2020-01-01')
>>> print(employee.name)
Fred
>>> print(employee.start_date)
datetime.date(2020, 1, 1)
>>> print(as_dict(employee))
{'id': None, 'name': 'Fred', 'start_date' = datetime.date(2020, 1, 1)}
```

#### insert

The new `employee` instance only exists in python&mdash;there is
nothing in the database yet.
Using a connection to the database, called a `cursor`,
the `employee` can be saved:

```
>>> await employee.save(cursor)
>>> print(employee.as_dict())
{'id': 123, 'name': 'Fred', 'start_date' = datetime.date(2020, 1, 1)}
```

Now the `employee` instance is saved and has
been automatically assigned the `id` of 123 by the database.

What happened? The `save` method determined that since
`employee` didn't have a primary key value,
an `INSERT` would have to be performed.
The `SQL` statement can be examined by inspecting the `cursor`'s
`query` and `query_after` attributes, which hold the most recent
operation performed against the database:

```
>>> print(cursor.query)
INSERT INTO `employee` ( `name`,`start_date` ) VALUES ( %s,%s )
>>> print(cursor.query_after)
INSERT INTO `employee` ( `name`,`start_date` ) VALUES ( 'Fred','2020-01-01' )
```

#### update

If `employee` is changed:

```
employee.name = 'Fred Mercury'
```

then `save` will detect the change, and perform an `UPDATE`:

```
>>> await employee.save(cursor)
>>> cursor.query
'UPDATE  `employee` SET `name`=%s WHERE  `id`=%s'
```

Only the changed field, `name`, is part of the `UPDATE`.
If no fields are changed, then there is no interaction with the database.
All of this is managed by the `Model`.

## Model Methods

#### `__init__`

The default constructor for a `Model` accepts keyword arguments
for each defined `Field`.
Required fields are enforced; otherwise, defaults are assigned.

If you define your own constructor, be sure to use `super`.

#### save - `save(cursor, force_insert=True)`

The `save` method saves any changes in the `Model` to the database
referenced by `cursor`.
If the `Model` doesn't have a value for the primary key,
then an `INSERT` is performed; otherwise, an `UPDATE` is performed.

To force `INSERT` even if the primary key has a value,
set `force_insert` to `True`.

When an `INSERT` happens, if the database is responsible for
generating the primary key, then that new primary key is stored in the
primary key attribute of the `Model`.

When an `UPDATE` happens, only the changed fields will be
part of the `UPDATE`.
If nothing is changed, then an `UPDATE` is not executed.
After the `save` completes,
the names and values of the updated fields are available
using the `get_updated` helper function.

#### load - `load(cursor, primary_key)`

The `load` classmethod creates a single instance of `Model` from the database
referenced by `cursor`, by doing a `SELECT` by primary key.

#### delete - `delete(cursor)`

The `delete` method deletes a single row from the database
referenced by `cursor`, by doing a `DELETE` by primary key.

#### query

## Helper Functions

#### get_updated

The `get_updated` function takes a `Model` instance and returns
a `dict` of changed fields from the most recent `save`.
The result contains one key for each changed field whose associated value is
a `tuple` of (`old_value`, `new_value`).

#### as_dict

The `as_dict` function takes a `Model` instance and returns
a `dict` of `field_name`, `field_value` pairs.
