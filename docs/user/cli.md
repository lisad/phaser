# Using the CLI

## Running pipeline with CLI

Phaser includes a command-line tool that can run an existing pipeline on a new source.
As an example, if you have cloned the phaser repository, you can run the
`EmployeeReviewPipeline` in the `tests/pipeline` directory.  Running on the
command-line will produce the warnings and errors with row numbers that persist
throughout the pipeline.

```
% cd tests
% python -m phaser run employees ../phaser_output fixture_files/employees.csv
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

## Showing the 'diffs' or changes made by phases or entire pipeline

After running a pipeline and having its output saved in a working directory (for example with the pipeline
run command above), the 'diff' tool can show exactly what changed with each Phase and over the whole Pipeline,
in a table-aware format.

```
% cd tests
% python -m phaser run employees ~/phaser_output
Diff of source and Validator_output_employees.csv will be saved in ../phaser_output/diff_to_Validator.html
    0 rows added
    1 rows removed
    3 rows changed
    0 rows unchanged
Diff of Validator_output_employees.csv and Transformer_output_employees.csv will be saved in ../phaser_output/diff_to_Transformer.html
    0 rows added
    0 rows removed
    3 rows changed
    0 rows unchanged
Entire pipeline changes in ../phaser_output/diff_pipeline.html
    0 rows added
    1 rows removed
    3 rows changed
    0 rows unchanged
```

After printing these summary statistics, the diff tool automatically opens an HTML file with links to each diff file
formatted as HTML.
