import pytest
import os
import pandas
from pathlib import Path
from pipelines.employees import EmployeeReviewPipeline

current_path = Path(__file__).parent


def test_employee_pipeline(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    EmployeeReviewPipeline(source=source, working_dir=tmpdir).run()
    assert os.path.exists(tmpdir / 'Validator_output_employees.csv')
    assert os.path.exists(tmpdir / 'Transformer_output_employees.csv')


def test_employee_pipeline_calculates_bonus(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    EmployeeReviewPipeline(source=source, working_dir=tmpdir).run()
    new_data = pandas.read_csv(tmpdir / 'Transformer_output_employees.csv').to_dict(orient='records')
    assert all([row['Bonus percent'] > 0.1 and row['Bonus percent'] < 0.2 for row in new_data])


# ALso add tests that introduce errors to employee data & run pipeline or phases?
