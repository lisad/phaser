# Usage

This document should explain the basics of using phaser.

## Column Declarations

The data that you bring through your pipeline may include dates, integers, floats, booleans or other data-types.
These types, as well as strings and custom types, can be validated and fixed using the [Column](#Column) declarations
and its subclasses.
Column logic is applied at the beginning of a __Phase__, and every column is evaluated independently.  Validations or
transformations that require multiple columns should be done in steps, not column declarations.

The benefit of doing column logic first in every __Phase__, besides consistency, is that the Column
declarations prepare and validate the data for every subsequent step to depend upon.  If the Column
declaration says that a column's values cannot be null or zero, then steps to do math on those values don't have
to check for null or zero.

The example below casts an integer and a float column so
that they can be used in math steps.   In addition, the count column will get renamed in case sources
are inconsistent about how columns are named, the price column must not be zero, sale type values get capitalized, 
and sale type is checked against an enumeration of allowed values.

```
IntColumn('item_count', null=False, default=0, rename='count')
FloatColumn('unit_price', required=True, null=False, min_value=0.01)
Column('sale_type', 
       allowed_values=['Regular', 'Final', 'Exchange'], 
       fix_value_fn='capitalize')
```

Column declarations have a number of features that save phaser users from writing (and testing, and fixing) code, 
so it's worth reading up on the parameters of different Column types.

* [Column](#Column)
* [BooleanColumn](#BooleanColumn)
* [IntColumn](#IntColumn)
* [FloatColumn](#FloatColumn)
* [DateTimeColumn](#DateTimeColumn)
* [DateColumn](#DateColumn)


## Row steps

## Batch steps

## DataFrame steps

## Built-in steps

## Running in production

There are two approaches to using Phaser in a production environment --
programmatic invocation or writing scripts that use the command-line interface.

**TODO** Talk about when programmatic invocation makes sense, such as if you are
using a job running or orchestration tool.

Phaser can be launched from any python program by importing your pipeline,
instantiating it with today's data file(s) and working directory, and running
it.  Output will be saved in the working directory along with errors, warnings,
and checkpoints (a copy of the data as it appeared at the end of each phase, so
that changes can be traced back to the phase they occurred in)

As an example, if you have cloned the phaser repository, you can run the
`EmployeeReviewPipeline` in the `tests.pipeline` package.

```python
from tests.pipelines.employees import EmployeeReviewPipeline

pipeline = EmployeeReviewPipeline(
        source='tests/fixture_files/employees.csv',
        working_dir='~/phaser_output'
    )
pipeline.run()
```
