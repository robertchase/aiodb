# Field
### aiodb column definition

## Introduction

When creating a model of a database table,
a `Field` is used to indicate each mapped
column or expression.
Not all columns need to be mapped, and not all
`Field`s represent underlying columns.

Here is a trivial example:
```
class MyTable(Model):
    id = Field(Integer)
    my_value = Field(expression='some_column * 4')
```

This model defines an integer column named `id`
and an expression called `my_value`. When this
model is used to query the database, `my_value`
will always be four times the value in `some_column`.

This model can be used to update the `id`, but not
`my_value` (expressions are always
read-only attributes of a model).

## Field Types

Every `Field` has a `type` which translates the `Field` value
back and forth between the database and python.

The default `type` is `aiodb.String`.

#### String `aiodb.String`

A `String` field is used for any column that holds
a non-binary character value.

#### Char(length, strict=True) `aiodb.Char`

A `Char` field is used for any column that holds a `String`
value with a maximum length.
If `strict` is `True`,
a `Char` field will not accept a value that exceeds the maximum length;
otherwise, the value will be truncated to the maximum length.

The length is specified during `Field` definition.
Here is an example of a `name` field being specified
as a `Char` of up to 100 characters:

```
    class MyTable(Model):
        name = Field(Char(100))
```

#### Enum(e1, e2, ...) `aiodb.Enum`

An `Enum` field is used for any column that holds an Enum value.
An `Enum` field type is specified with a list of acceptable values.

Here is an example of a `status` field being specified
as an `Enum`:

```
    class MyTable(Model):
        status = Field(Enum('NEW', 'WORKING', 'DONE'))
```

#### Integer `aiodb.Integer`

An `Integer` field is used for any column that holds
an integer value of any size.
An `Integer` field will not accept a float assignment.

#### Boolean `aiodb.Boolean`

An `Boolean` field is used for any column that holds
a boolean value.
A `Boolean` field will accept a python `True` or `False`,
a string 'TRUE' or 'FALSE' in any mixture of case,
a string 'T', 't', 'F', or 'f' or a zero or one `int` value.

The underlying boolean column is usually a small integer value.

#### Date `aiodb.Date`
A `Date` field is used for any column that holds
a date value.
A `Date` field will accept a `datetime.datetime`,
a `datetime.date`
or a string date in iso format (`CCYY-MM-DD`).

The underlying date column is usually some sort of date
representation, but can also be a datetime.

#### Datetime `aiodb.Datetime`
A `Datetime` field is used for any column that holds
a datetime value.
A `Datetime` field will accept a `datetime.datetime`,
a `datetime.date`
or a string date in 'CCYY-MM-DD HH:MM:SS.000...' or
'CCYY-MM-DD HH:MM:SS' format.

The underlying datetime column may need to be specified
correctly to accept values that have precision greater than a second.

#### Binary `aiodb.Binary`
A `Binary` field is used for any column that holds
a binary data.

## Field Parameters

The simplest way to define a field is with:
```
Field()
```
This `Field` holds a non-nullable string value.

Here is the full `Field` definition:
```
Field(
    type=None,
    default=None,
    column=None,
    is_nullable=False,
    is_primary=False,
    foreign=None,
    expression=None,
    is_readonly=False,
    is_database=True
)
```

#### type
Specifies the class
that enforces values assigned to the underlying attribute.

#### default
Specifies a default value for the attribute.

#### column
Specifies the name of the underlying database column.
When the `Field` is assigned to a `Model` attribute,
the attribute name is typically used as the column name.
When that is not possible&mdash;for instance,
when the column name is a python reserved word&mdash;the
column parameter can be used instead.

#### is_nullable
If a `Field` is not nullable, then it will not accept
a `None` assignment.

#### is_primary
Specifies the `Field` as the table's primary key.

This will enable automatic assignment of an auto-generated primary key
if supported in the underlying column.
In other words, the primary key generated by the database on `INSERT`
will be placed in the `Model`'s `is_primary` attribute.

#### foreign
Specifies a foreign relationship to another table.
This field can be set to the class name of the `Model` for the related table,
or a dot-delimited path to the model (eg,
'myapp.model.mytable.MyTable').

The purpose of the foreign key specification is to inform the
`join` method of a `query` when connecting tables.

#### expression
Specifies a SQL expression that is run when the `Model`
is used to execute a query.
An expression `Field` is automatically read-only.

#### is_readonly
If a `Field`
is read-only, then the attribute cannot be assigned.

#### is_database
Specifies a `Model` attribute that is not tied to the database.
