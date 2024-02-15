# phaser

A library to simplify automated batch-oriented complex data integration pipelines, by 
organizing steps and column definitions into phases, and offering utilities for 
transforms, sorting, validating and viewing changes to data. 

## Goals and Scope

This library is designed to help developers run a series of steps on _batch-oriented_,
_record-oriented_, un-indexed data.  A batch of record-oriented data means a set of records
that are intended to be processed together, in which each record has more or less the same
fields and those fields are the same type across records.  Often record-oriented data can
be expressed in CSV files, where the first line
contains the column names to associate with all the fields in rows in all the other lines.
Record-oriented data can be stored or expressed in various formats and objects including:
* CSV files
* Excel files
* Pandas dataframes
* JSON files, provided the JSON format is a list of dicts

In this project, record-orientation is somewhat forgiving.  The library does not insist that
each row must have a value for every column.  When some records don't have some fields we can
call that 'sparse' data. It may sometimes be represented in a format that isn't columnar
(a JSON format might easily contain records in which only fields with values are listed).  Sparse
record-oriented data should be trivial to handle in this library, although by default checkpoint
data will be saved in a columnar way that shows all the null values.

The goals of Phaser are to offer an opinionated framework with a structure that
* shortens the loop on debugging where a complex data integration is failing
* empowers teams to work on the same code rather than only one assigned owner/expert
* makes refactoring and extending data integration code easier
* reduces error rates

The mechanisms that we think will help phaser meet these goals:
* make it easy to start using phaser without changing everything
* default choices and tools that support shortened-loop debugging
* encourage code organized in very testable steps and testable phases, via sample code and useful features
* make it easy to add complexity over time and move gracefully from default to custom behaviour
* make high-level code readable in one place, as when a Phase lists all of its steps declaratively
* tools that support visibility and control over warnings and data changes

## Simple example

```
class Validator(Phase):
    columns = [
        Column(name="Employee ID", rename="employeeNumber"),
        Column(name="First name", rename="firstName"),
        Column(name="Last name", rename="lastName", blank=False),
        FloatColumn(name="Pay rate", min_value=0.01, rename="payRate", required=True),
        Column(name="Pay type",
               rename="payType",
               allowed_values=["hourly", "salary", "exception hourly", "monthly", "weekly", "daily"],
               on_error=Pipeline.ON_ERROR_DROP_ROW,
               save=False),
        Column(name="Pay period", rename="paidPer")
    ]
    steps = [
        drop_rows_with_no_id_and_not_employed,
        check_unique("Employee ID")
    ]


class Transformer(Phase):
    columns = []
    steps = [
        combine_full_name,
        calculate_annual_salary,
        calculate_bonus_percent
    ]


class EmployeeReviewPipeline(Pipeline):

    phases = [Validator, Transformer]

```

The example above defines a validation phase that renames a number of columns and defines their values, a 
transformer phase that performs calculations, and a pipeline that combines both phases.  The full example 
can be found in the tests directory of the project, including the sample data and the custom steps defined.

The benefit of even such a simple pipeline expressed as two phases is that the phases can be debugged, tested and
run separately. A developer can run the Validator phase once then work on adding features to the Transformer phase,
or narrow down an error in production by comparing the checkpoint output of each phase.  In addition, the code
is readable and supports team collaboration.


## Contributing

To set up project for contributing:
* python -m venv venv
* source venv/bin/activate
* pip install -r requirements.txt
* pip install -e .  # installs the phaser library (in edit mode!) so pytest can import it

Then run:
* pytest


## Features

The construction of a Phase instance means that you can put a bunch of data transformation or
data testing steps in a series, and the Phase does routine work for you in a robust way:

* it will load your data from a source file or a previous phase
* it will canonicalize field names to lowercase and strip dangerous characters
* it will run your steps row-by-row in order
* it will save your results to a different file, usable as a checkpoint
* it will report errors or warnings as summaries 

Columns can be passed and formats and limits enforced at the beginning and at the end of Phases.
Many steps that might otherwise have been programmed functionally are therefore available to
declare.  Columns available so far:

* IntColumn
* FloatColumn
* DateColumn
* DateTimeColumn

Then, any remaining work can be done in granular steps, which operate row by row (rows are Python dicts).

* Pre-baked steps are available to check uniqueness values and do common transforms
* Steps can drop rows with bad data
* Steps can access context information
* Steps can create warnings or errors

Phases that reshape the entire dataset or operate on the entire dataset are
handled separately because the error reporting, error handling and diffs are different.
