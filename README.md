# phaser

A library to simplify automated batch-oriented complex data integration pipelines

This library is designed to help developers run a series of steps on _batch-oriented_,
_record-oriented_, un-indexed data.  A batch of record-oriented data means a set of records
that are intended to be processed together, in which each record has more or less the same
type and fields (note exceptions LMDTODO).  Often record-oriented data can be expressed in
CSV files, where the first line
contains the column names to associate with all the fields in rows in all the other lines.
Record-oriented data can be stored or expressed in various formats and objects including:
* CSV files
* Excel files
* Pandas dataframes
* JSON files, provided the JSON format is a list of dicts

The goals of Phaser are
* to offer an opinionated framework with a structure that improves maintainability, especially
by shortening the loop on debugging where a complex data integration is failing.
* to encourage code organized in very testable steps
* to enable iteration rather than epic rewrites
* to empower teams to maintain complex data integrations through readable declarative code


To setup project for contributing:
* python -m venv venv
* source venv/bin/activate
* pip install -r requirements.txt
* pip install -e .  # installs the phaser library (in edit mode!) so pytest can import it

Then run:
* pytest


# Using Phaser

The construction of a Phase instance means that you can put a bunch of data transformation or
data testing steps in a series, and the Phase does routine work for you in a robust way:
* it will load your data from a source file or a previous phase
* it will canonicalize field names to lowercase and strip dangerous characters (LMDTODO)
* it will run all your steps row-by-row or across the whole dataset
* it will save your results to a different file, usable as a checkpoint
* it will report errors or warnings as summaries (LMDTODO)

In addition this library organizes a variety of kinds of steps :
* Prebaked steps to check uniqueness values and do common transforms (LMDTODO)
* Step wrappers to control errors and warnings (LMDTODO)
* Steps that operate on rows represented as Python dicts
* Steps that operate on pandas DataFrames (LMDTODO)
