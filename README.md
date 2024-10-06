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

Phaser comes with table-sensitive diff tooling to make it very easy to develop and debug phases.  The output
of the diff tool looks something like this (making allowances for shoving HTML into Markdown),
viewing the pipeline results above operating on one of phaser's text fixture files:

<div>
        <style type="text/css">
            table { font-family: Arial; padding: 20px; }
            table td { text-align: end; padding:8px; }
            table th { 
                text-transform: uppercase;
                padding:8px;
                border-bottom: 1px solid #e8e8e8;
                font-size: 0.8125rem; 
            }
            .newtext { color: green; text-decoration: underline }
            .deltext { color: red; text-decoration: line-through }
        </style>
    <table><tr>
<th><!--change type--></th>
<th>Row number</th>
<th><span class="deltext">employeeNumber</span><br/><span class="newtext">Employee ID</span></th>
<th><span class="deltext">firstName</span><br/><span class="newtext">First name</span></th>
<th><span class="deltext">lastName</span><br/><span class="newtext">Last name</span></th>
<th>payType</th>
<th><span class="deltext">paidPer</span><br/><span class="newtext">Pay period</span></th>
<th><span class="deltext">payRate</span><br/><span class="newtext">Pay rate</span></th>
<th>bonusAmount</th>
<th>Status</th>
<th>Full name</th>
<th>salary</th>
<th>Bonus percent</th></tr>
<tr><td><i>Changed</i></td>
<td>1</td>
<td>1</td>
<td>Benjamin</td>
<td>Sisko</td>
<td><span class="deltext">salary</span></td>
<td>Year</td>
<td>188625.0</td>
<td>30000<span class="newtext">.0</span></td>
<td>Active</td>
<td><span class="newtext">Benjamin Sisko</span></td>
<td><span class="newtext">188625.0</span></td>
<td><span class="newtext">0.15904572564612326</span></td></tr><tr><td><i>Changed</i></td>
<td>2</td>
<td>2</td>
<td>Kira</td>
<td>Nerys</td>
<td><span class="deltext">salary</span></td>
<td>Year</td>
<td>118625.0</td>
<td>20000<span class="newtext">.0</span></td>
<td>Active</td>
<td><span class="newtext">Kira Nerys</span></td>
<td><span class="newtext">118625.0</span></td>
<td><span class="newtext">0.16859852476290832</span></td></tr>
<tr><td><span style="color: grey"><i>Deleted</i></span></td>
<td>3</td>
<td><span style="color: grey"></span></td>
<td><span style="color: grey">None</span></td>
<td><span style="color: grey">Garak</span></td>
<td><span style="color: grey">salary</span></td>
<td><span style="color: grey">Year</span></td>
<td><span style="color: grey"> 100000</span></td>
<td><span style="color: grey"></span></td>
<td><span style="color: grey">Inactive</span></td>
<td><span style="color: grey"></span></td>
<td><span style="color: grey"></span></td>
<td><span style="color: grey"></span></td></tr>
<tr><td><i>Changed</i></td>
<td>4</td>
<td>3</td>
<td>Julian</td>
<td>Bashir</td>
<td><span class="deltext">salary</span></td>
<td>Year</td>
<td>142880.0</td>
<td>25000<span class="newtext">.0</span></td>
<td>Active</td>
<td><span class="newtext">Julian Bashir</span></td>
<td><span class="newtext">142880.0</span></td>
<td><span class="newtext">0.17497200447928332</span></td></tr></table>
</div>

## Advanced Example

For a real, working advanced example, see the [phaser-example](https://github.com/lisad/phaser-example) repository on GitHub.
You should be able to clone that repository, fetch the Boston and Seattle bike trail bike sensor data,
and run the pipelines on the source data to get the data in a consistent format.

The pipelines in the phaser-example project demonstrate these features:

* Columns that get renamed
* Columns with allowed_values (enumerated types),
* Dropping columns,
* Dropping many rows (without creating many warnings),
* Sorting,
* Adding columns,
* Using pandas 'aggregate' method to sum values distributed across rows, within a phaser step
* Pivoting the data by timestamp column into long row-per-timestamp data format,
