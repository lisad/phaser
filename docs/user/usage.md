# Usage

This document should explain the basics of using phaser.

## Getting data into and out of the Pipeline

A Pipeline class comes with data management functionality built-in.  Each Pipeline is meant to be run on one
named source at a time.  The command line tool (#CLI) can be used to run the pipeline on a source file. In the 
simplest cases, just include the source file name in the CLI invocation.  Also, the example below shows a 
slightly more advanced invocation suitable for scripting, where the source files are in different directories by date.

```
> python -m phaser run my_pipeline /usr/bin/test_sourcedata.csv
or 
> python -m phaser run my_pipeline /mount/share/$(eval date "+%Y%m%d")/sourcedata.csv
```

If a Pipeline needs to be run within a python environment, the source file just needs to be part of instantiating
the Pipeline, and the working directory needs to be supplied as well.

```python
    pipeline = MyPipeline(working_dir=tmpdir, source=tmpdir / 'sourcedata.csv')
    pipeline.run()
```


Phaser also automatically opens files with a 'json' extension, assuming the json 'record' format is used (where
the JSON document is a list at the top level, and each item in the list is a dict representing a record).  

```
> python -m phaser run my_pipeline /Users/james/latest_data.json
INFO:phaser.pipeline:ExtractPhase saved output to output/ExtractPhase_output.csv
```

After running the pipeline, the 'output' directory will be created inside the working directory,  with the output of 
every Phase and a copy of the source data, plus a file with errors and warnings.  Although only the final output may 
be needed when the pipeline is successful, the rest of the files can be useful for debugging.  

Note as hinted in the previous example that Pipeline defaults to saving checkpoint data and output data as CSV unless 
the preferred output format is overridden.  This is how the output format can be overridden:

```python


class MyPipeline(Pipeline):
    save_format = JSON_RECORD_FORMAT
    phases = [MyPhase]

```

With the save_format overridden, the pipeline will now save checkpoint and output data as JSON, though it may still
import data in CSV files by detecting the file extension.

## Validating/casting fields with Column Declarations

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


## Declarative column fixing

Column declarations can have common operations passed in as a list of functions to apply to the column values
to "fix" them.  For example, it's pretty common to have users type in values like "new", "NEW" and "New",
so asking the value to always be converted to upper-case via the 'upper' method makes those consistent.

```
Column('status', fix_value_fn='upper')
```

These can also be in a list of functions that are applied in order:

```
Column('status', fix_value_fn=['strip', 'upper'])
```

Note that the column type is applied first, so the functions must work on the given data type.  The following works:

```
IntColumn('value', fix_value_fn='abs')
```

But the next example will give an error because python 'strip' doesn't work on dates:

```
ERR: DateColumn(name='date', fix_value_fn='strip')
```

Happily, the above example will convert ' 2023-01-01 '  to a date without explicitly asking it to strip spaces.
Should you require a "fix_value_fn" that applies before the column is cast to its data type, a custom Column class
may be required instead (see [custom validation for columns](#custom-column-validation))

## Dropping columns

Columns can be dropped individually by telling a Phase not to save them in its output:

```python
Column('deviceId', save=False)
```

Declaring the column is going to make the Phase code look for it at the beginning of the Phase, even if it's going
to leave it out when saving the output of the Phase (after all, you might need to use the column in a step before
dropping it at the end).  Because of this, if JSON data occasionally contains a field that you don't want to keep,
it needs to be marked as optional as well as not saved.

```python
Column('expectedNormal', required=False, save=False)
```

JSON data can be flattened with builtin steps, creating new columns out of nested data in the middle of the phase
(see [flatten_column](#flattening_json)).  In the example below, if the 'payload' value has nested values including
'note', declaring the 'payload__note' column as not required and not saved will cause it to be dropped at the end of
the Phase.

```python
class ExtractPhase(Phase):
    columns = [Column('payload__note', required=False, save=False)]
    steps = [flatten_column('payload')]

```

## Row steps

Row steps are steps that can be declared for the Phase to run on the data one row at a time. Row steps can be 
custom-written to do any logic that requires on the data in one row at a time.  This example finds the hour of day
from a column that has already been declared as a phaser.DateTimeColumn.

```python
@row_step
def find_hour_bucket(row, context):
    row['hour'] = row['time'].hour
    return row

```

Although row steps aren't meant to consider data from multiple rows, they can use data from other sources or from 
the context. This simple example can be used to copy a required field that may be constant for the file as a whole into 
every row for when that row is imported into a database of mixed sources.  

```python
@row_step
def fill_user_name(row, context):
    # When run on a file, this pipeline sets up a context with the user_name of the user who submitted the file   
    row['contributor_user_name'] = context.get('user_name')
    return row
```

## Batch steps

Phaser has a number of built-in steps that operate on the whole batch.  This kind of step can be useful to 
have transformations that drop duplicate rows, or test that indexes that are supposed to be unique are in fact unique.

### Built-in batch steps

Some of the more common batch steps are already built-in, ready to use:
 * [drop_duplicate_rows](#drop_duplicate_rows) - rows that have the same values in one column, several columns or all 
columns can be dropped. E.g. to simply drop rows with duplicate 'id' values, use the step 'drop_duplicate_rows(['id'])
in a phase of your pipeline.
 * [check_unique](#check_unique) - rows that have the same value in key columns will cause exceptions to be raised
 * [sort_by](#sort_by) - can sort the batch of data by one or more columns
 * [filter_rows](#filter_rows) - can keep only rows that match a value test, e.g. `filter_rows(lambda row: 
row['id'] is not None)`

```python
import phaser
# Example of using a built-in steps. This phase uses only built-in 
# steps and Column attributes to reduce the incoming blood-glucose 
# monitor data to a smaller set of useful columns and rows.

class ExtractPhase(phaser.Phase):
    columns = [
        phaser.Column('deviceId', save=False),
        phaser.Column('uploadId', save=False),
        phaser.Column('guid', save=False),
        phaser.Column('clockDriftOffset', save=False),
        phaser.Column('conversionOffset', save=False)
    ]
    steps = [
        phaser.filter_rows(lambda row: row['type'] in ['cbg', 'basal']),
        phaser.drop_duplicate_rows(['type', 'timestamp'])
    ]
```

### Writing a custom batch step

A custom batch step accepts a batch of data in record format (list of dicts).  It returns the data in the same format.
The following example returns the data unchanged unless the variance of one column is too high for the pipeline to
continue.

```python
import phaser

def variance(array):
    mean = sum(array) / len(array)
    return sum((value - mean) ** 2 for value in array) / len(array)

@phaser.batch_step
def error_tachyon_level_variance(batch, context):
    tachyon_values = [row['tachyon_level'] for row in batch]
    if variance(tachyon_values) > 10:
        raise Exception("Tachyon variance at high levels")
    return batch

```

A batch step may add or remove rows, but if it does, add `check_size=False` to the batch_step declaration.  Otherwise,
the decorator will check whether there are the same number of rows as before running the step, to avoid common errors
where conditional logic is used and the developer accidentally only returns some rows.  Note that the decorator
also checks to make sure the batch data is returned (explicitly returning it helps the developer if they 
accidentally forgot to return the data, modified or unmodified) and to make sure the batch data is returned in 
a format that Phaser can use.

```python
import phaser

@phaser.batch_step(check_size=False)
def separate_count_values_into_different_rows(batch, context):
    new_data = []
    for row in batch:
        new_data.append({'timestamp': row['timestamp'], 'count': row['northbound_count']})
        new_data.append({'timestamp': row['timestamp'], 'count': row['southbound_count']})
    return new_data
```

## DataFrame steps

## Built-in steps

(flattening_json)=
### JSON utilities: flatten_column and flatten_all

These two built-in steps are helpful in transforming nested JSON data into values that can be directly processed.

```python
import phaser
#Source data sample row:
#    {'time': "2024-11-18 19:10:29+00:00",
#     'value': 8.77018,
#     'units': 'mmol/L',
#     'payload': {
#         'timestamp': 532858229,
#         'logIndices': ['123456'],
#         'rate': -1,
#         'rssi': -90
#     }
#    }    

class FlattenPhase(phaser.Phase):
    steps = [
        phaser.flatten_column('payload')
    ]

# Phase output
# time, value, units, payload__timestamp, payload__logIndices, payload__rate, payload__rssi
# "2024-11-18 19:10:29+00:00", 8.77018, "mmol/L", 532858229, ['123456'], -1, -90
```

In this case the same result can be achieved with the flatten_all builtin method, because there's only one 
column with a value that is a dict.  The value that is a list is not altered by the flatten methods.

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
