# Concepts

Phaser was designed with only a few concepts to make managing data pipelines as
simple as possible.

## Pipelines

[Pipeline](#Pipeline) is the main organizing unit that is used to define the
structure of the work to be done.  It runs one or more [Phases](#phases) and
does I/O, marshalling source data and checkpoint data between them.  It will

* load source data from files or a previous phase
* save checkpoint data between phases
* save outputs
* marshal inputs and outputs between phases
* report errors or warnings as summaries
* captures and handles errors, according to the error policy

Errors and warnings are output to a file in the working directory by default.

## Phases

Each [Phase](#Phase) runs one or more [steps](#steps) with individual data
transformation or validation logic, and the Phase does routine work in a robust
way:

* transform column headers to preferred name/case
* routine parsing and data typing

### Columns

A Phase can be configured with the data that it expects, defined as
[Columns](#Column).

When columns are passed to the Phase, then the data formats and constraints are
enforced at the beginning of a Phaser.  Many steps that might otherwise have
been programmed functionally are therefore available to declare.

Columns and features available so far:

* [BooleanColumn](#BooleanColumn)
* [IntColumn](#IntColumn), [FloatColumn](#FloatColumn)
* [DateColumn](#DateColumn), [DateTimeColumn](#DateTimeColumn)
* Range validation, list of allowed values
* Checking for nulls/blanks, assigning default values

## Steps

For your data pipeline project, most of the work unique to that project can be
done within steps that operate in a Phase to give structure and debuggability:

* Steps are meant to be written as pure functions so they can be individually
  testable with simple pythonic ways to pass row data and verify results
* Steps can drop rows with bad data
* Steps can access [context](#phaser.Context) information
* Steps can create warnings or errors
* Pre-baked steps are available to check uniqueness values and do common transforms

## Checkpoint files

## Comparison to other tools and libraries
