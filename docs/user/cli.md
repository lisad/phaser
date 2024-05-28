# Running with the CLI

Phaser includes a command-line tool that can run an existing pipeline on a new source.


As an example, if you have cloned the phaser repository, you can run the
`EmployeeReviewPipeline` in the `tests/pipeline` directory.  Running on the
command-line will produce the warnings and errors with row numbers that persist
throughout the pipeline.

```
% cd tests
% python -m phaser run employees ~/phaser_output fixture_files/employees.csv
Running pipeline 'EmployeeReviewPipeline'

% cat ~/phaser_output/errors_and_warnings.txt
-------------
Beginning errors and warnings for Validator
-------------
DROPPED_ROW in step drop_rows_with_no_id_and_not_employed, row 3: message: 'DropRowException raised (Employee Garak has no ID and inactive, dropping row)'
-------------
Beginning errors and warnings for Transformer
-------------
WARNING in step consistency_check, row 1: message: 'New field 'Full name' was added to the row_data and not declared a header'
WARNING in step consistency_check, row 1: message: 'New field 'salary' was added to the row_data and not declared a header'
WARNING in step consistency_check, row 1: message: 'New field 'Bonus percent' was added to the row_data and not declared a header'
```
