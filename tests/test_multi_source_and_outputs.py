import pytest
import io
from contextlib import redirect_stdout
import os
import pandas
from pathlib import Path
from pipelines.multi_source_and_outputs import EmployeeEnrichPipeline
from phaser.io import read_csv

current_path = Path(__file__).parent


def test_pipeline(tmpdir):
    source = current_path / "fixture_files" / "more-employees.csv"
    department_source = current_path / "fixture_files" / "departments.csv"
    pipeline = EmployeeEnrichPipeline(source=source, working_dir=tmpdir)
    pipeline.init_source('departments', department_source)
    pipeline.run()

    assert os.path.exists(tmpdir / 'Validation_output.csv')
    assert os.path.exists(tmpdir / 'Transformation_output.csv')
    assert os.path.exists(tmpdir / 'Enrichment_output.csv')
    assert os.path.exists(tmpdir / 'managers.csv')

    new_data = read_csv(tmpdir / 'Enrichment_output.csv')
    assert len(new_data) == 5 # One employee should be dropped
    assert all([float(row['Bonus percent']) > 0.1 and float(row['Bonus percent']) < 0.2 for row in new_data])

    assert new_data[0]['department_id'] == '2'
    assert new_data[1]['department_id'] == '1'
    assert new_data[2]['department_id'] == '2'
    assert new_data[3]['department_id'] == '1'
    assert new_data[4]['department_id'] == '1'

    assert new_data[0]['manager_id'] == '4'
    assert new_data[1]['manager_id'] == ''
    assert new_data[2]['manager_id'] == ''
    assert new_data[3]['manager_id'] == '2'
    assert new_data[4]['manager_id'] == '2'

    manager_data = read_csv(tmpdir / 'managers.csv')
    assert len(manager_data) == 2
    # TODO: Allow ExtraMappings to specify the names for keys and values
    # assert manager_data == [
    #     { 'manager_id': '4', 'num_employees': '1' },
    #     { 'manager_id': '2', 'num_employees': '2' },
    # ]
    assert manager_data == [
        { 'key': '4', 'value': '1' },
        { 'key': '2', 'value': '2' },
    ]

    file_data = open(pipeline.errors_and_warnings_file(), 'r').read()
    assert "Beginning errors and warnings for Validation" in file_data
    assert "Employee Garak has no ID and inactive" in file_data
    assert "Beginning errors and warnings for Transformation" in file_data
    assert "'Full name' was added to the row_data and not declared a header'" in file_data

    # The extra output should be listed in the expected outputs.
    assert 'managers.csv' in pipeline.expected_outputs()
