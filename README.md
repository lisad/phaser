# phaser

A library to simplify automated batch-oriented complex data integration pipelines, by 
organizing steps and column definitions into phases, and offering utilities for 
transforms, sorting, validating and viewing changes to data. 

## Goals and Scope

This library is designed to help developers run a series of steps on _batch-oriented_,
_record-oriented_, un-indexed data.  A batch of record-oriented data is a set of records
that are intended to be processed together, in which each record has more or less the same
fields and those fields are the same type across records.  Often record-oriented data can
be expressed in CSV files, where the first line contains the column names.

Record-oriented data can be stored or expressed in various formats and objects including:

* CSV files
* Excel files
* Pandas dataframes
* JSON files, provided the JSON format is a list of dicts

In this project, record consistency is somewhat forgiving.  The library does not insist that
each record must have a value for every column.  Some records may not have some fields, i.e. 'sparse' data.
Sparse data may sometimes be represented in a format that isn't columnar
(a JSON format might easily contain records in which only fields with values are listed).  Sparse
record-oriented data should be trivial to handle in this library, although by default checkpoint
data will be saved in a columnar CSV that shows all the null values.

The goals of Phaser are to offer an opinionated framework for complex data pipelines with a structure that

* shortens the loop on debugging where a record has the wrong data or a step is failing
* empowers teams to work on the same code rather than only one assigned owner/expert
* makes refactoring and extending data integration code easier
* reduces error rates

The mechanisms that we think will help phaser meet these goals:

* make it easy to start using phaser without changing everything
* provide defaults and tools that support shortened-loop debugging
* encourage code organized in very testable steps and testable phases, via sample code and useful features
* make it easy to add complexity over time and move gracefully from default to custom behaviour
* make high-level code readable in one place, as when a Phase lists all of its steps declaratively
* tools that support visibility and control over warnings and data changes

## Simple example

```python
from phaser import Phase, Column, FloatColumn, Pipeline

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
    columns = [
        FloatColumn(name='Pay rate'),
        FloatColumn(name="bonusAmount")
    ]
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

## Running

Phaser can be launched from any python program by importing your pipeline, instantiating it with today's data
file(s) and working directory, and running it.  Output will be saved in the working directory along with
errors, warnings, and checkpoints (a copy of the data as it appeared at the end of each phase, so that
changes can be traced back to the phase they occurred in)

```python
from tests.pipelines.employees import EmployeeReviewPipeline

pipeline = EmployeeReviewPipeline(source='tests/fixture_files/employees.csv', working_dir='employee_output')
pipeline.run()

```

The package also includes a command-line tool that can run an existing pipeline on a new source.  The same pipeline
can be run from within the 'tests' directory if you have cloned the phaser repository, and produces its warnings and 
errors with row numbers that persist throughout the pipeline.

```
 % python -m phaser run employees ~/phaser_output fixture_files/employees.csv

Running pipeline 'EmployeeReviewPipeline'
Reporting for phase Validator
DROPPED row: 3, message: 'Employee Garak has no ID and inactive, dropping row'
Reporting for phase Transformer
WARNING row: 1, message: 'New field 'Full name' was added to the row_data and not declared a header'
WARNING row: 1, message: 'New field 'salary' was added to the row_data and not declared a header'
WARNING row: 1, message: 'New field 'Bonus percent' was added to the row_data and not declared a header'
```

## Contributing

To set up project for contributing:
* python -m venv venv
* source venv/bin/activate
* pip install -r requirements.txt
* pip install -e .  # installs the phaser library (in edit mode!) so pytest can import it

Then run:
* pytest


## Features

A phaser [Pipeline](#Pipeline) organizes one or more Phases and does I/O, marshalling source data and
checkpoint data between Phases.  It will

* load source data from files or a previous phase
* save checkpoint data between phases
* save outputs
* marshall inputs and outputs between phases

Each Phase runs one or more steps with individual data transformation or validation
logic, and the Phase does routine work in a robust way:

* transform column headers to preferred name/case
* routine parsing and data typing
* report errors or warnings as summaries

Different kinds of Phases operate slightly differently:

* regular Phase operates row-by-row and reports errors/warnings by row
* regular Phase offer access to diffs to examine results or debug steps
* reshaping Phases have fewer restrictions to allow data sources to be combined, split or reshaped

Columns can be passed and formats and limits enforced at the beginning and at the end of Phases.
Many steps that might otherwise have been programmed functionally are therefore available to
declare.  Columns and features available so far:

* IntColumn, FloatColumn
* DateColumn, DateTimeColumn
* Range validation, list of allowed values
* Checking for nulls/blanks, assigning default values

For your data pipeline project, most of the work unique to that project can be done within steps
that operate in a Phase to give structure and debuggability:

* Steps are individually testable with simple pythonic ways to pass row data and verify results
* Steps can drop rows with bad data
* Steps can access context information
* Steps can create warnings or errors
* Pre-baked steps are available to check uniqueness values and do common transforms

