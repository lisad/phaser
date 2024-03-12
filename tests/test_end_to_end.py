import pytest
import io
from contextlib import redirect_stdout
import os
from pathlib import Path
from pipelines.employees import EmployeeReviewPipeline
from phaser.io import read_csv

current_path = Path(__file__).parent


def test_employee_pipeline(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    department_source = current_path / "fixture_files" / "departments.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    pipeline.init_source('departments', department_source)
    pipeline.run()
    assert os.path.exists(tmpdir / 'Validator_output_employees.csv')
    assert os.path.exists(tmpdir / 'Transformer_output_employees.csv')


def test_results(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    department_source = current_path / "fixture_files" / "departments.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    pipeline.init_source('departments', department_source)
    pipeline.run()
    new_data = read_csv(tmpdir / 'Transformer_output_employees.csv')
    assert len(new_data) == 5 # One employee should be dropped
    assert all([row['Bonus percent'] > 0.1 and row['Bonus percent'] < 0.2 for row in new_data])
    assert len(pipeline.context.dropped_rows) == 1
    assert "Garak" in pipeline.context.dropped_rows[3]['message']

    assert new_data[0]['department_id'] == 2
    assert new_data[1]['department_id'] == 1

    manager_data = pandas.read_csv(tmpdir / 'managers.csv').to_dict(orient='records')
    assert len(manager_data) == 2
    assert manager_data == [
        { 'manager_id': 4, 'num_employees': 1 },
        { 'manager_id': 2, 'num_employees': 2 },
    ]


def test_reporting(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    f = io.StringIO()
    with redirect_stdout(f):
        # Having to grab stdout is probably temporary until we make pipeline more versatile in reporting errors
        pipeline.run()
    stdout = f.getvalue()
    assert "Reporting for phase Validator" in stdout
    assert "Employee Garak has no ID and inactive" in stdout
    assert "Reporting for phase Transformer" in stdout
    assert "WARNING row: 2, message: 'At some point, Full name was added to the row_data and not declared a header'"
