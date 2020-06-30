# Model
### aiodb model definition

## Introduction

A `Model` maps a database table to a python class.
What does this mean?
Simple operations like:
1. creating a new instance
2. saving a new or modified instance to the database
3. loading an instance from a database row

are easy to do.
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
from aiodb import Model, Field, Integer, Date

class Employee(Model):
    ___TABLENAME__ = 'employee'
    id = Field(Integer, is_primary=True)
    name = Field()
    start_date = Field(Date)
```

The model maps three fields, `id`, `name` and `start_date`.
Here is how a new `Employee` is created:

```
employee = Employee(name='Fred', start_date='2020-01-01')
print(employee.name)
>>> Fred
print(employee.start_date)
>>> 2020-01-01
print(employee.as_dict())
>>> {'id': None, 'name': 'Fred', 'start_date' = datetime.date(2020, 1, 1)}
```

The new `employee` instance only exists in python&mdash;there is
nothing in the database yet.
Using a connection to the database, called a `cursor`,
the `employee` can be saved:

```
employee.save(cursor)
print(employee.as_dict())
>>> {'id': 123, 'name': 'Fred', 'start_date' = datetime.date(2020, 1, 1)}
```

Now the `employee` instance is saved and has
been automatically assigned the `id` of 123 by the database.

What happened? The `save` method determined that since
`employee` didn't have a primary key value,
an `INSERT` would have to be performed.
The `SQL` statement can be examined by inspecting the `cursor`'s
`query` and `query_after` attributes:

```
print(cursor.query)
INSERT INTO `employee` ( `name`,`start_date` ) VALUES ( %s,%s )
print(cursor.query_after)
INSERT INTO `employee` ( `name`,`start_date` ) VALUES ( 'Fred','2020-01-01' )
```
