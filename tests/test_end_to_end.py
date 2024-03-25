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
    EmployeeReviewPipeline(source=source, working_dir=tmpdir).run()
    assert os.path.exists(tmpdir / 'Validator_output_employees.csv')
    assert os.path.exists(tmpdir / 'Transformer_output_employees.csv')


def test_results(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    pipeline.run()
    new_data = read_csv(tmpdir / 'Transformer_output_employees.csv')
    assert len(new_data) == 2 # One employee should be dropped
    assert all([float(row['Bonus percent']) > 0.1 and float(row['Bonus percent']) < 0.2 for row in new_data])


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
    assert "WARNING row: 2, message: 'At some point, Full name was added to the row_data and not declared a header'" in stdout
