import pytest
import io
from contextlib import redirect_stdout
import os
import pandas
from pathlib import Path
from pipelines.multi_source_and_outputs import EmployeeReviewPipeline
from phaser.io import read_csv

current_path = Path(__file__).parent


def test_employee_pipeline(tmpdir):
    print(tmpdir)

    source = current_path / "fixture_files" / "more-employees.csv"
    department_source = current_path / "fixture_files" / "departments.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    pipeline.init_source('departments', department_source)
    f = io.StringIO()
    with redirect_stdout(f):
        # Having to grab stdout is probably temporary until we make pipeline more versatile in reporting errors
        pipeline.run()
    stdout = f.getvalue()
    print(stdout)

    assert os.path.exists(tmpdir / 'Validator_output_more-employees.csv')
    assert os.path.exists(tmpdir / 'Transformer_output_more-employees.csv')
    assert os.path.exists(tmpdir / 'ManagerPhase_output_more-employees.csv')
    assert os.path.exists(tmpdir / 'managers.csv')

    new_data = read_csv(tmpdir / 'ManagerPhase_output_more-employees.csv')
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
    assert manager_data == [
        { 'manager_id': '4', 'num_employees': '1' },
        { 'manager_id': '2', 'num_employees': '2' },
    ]

    assert "Reporting for phase Validator" in stdout
    assert "Employee Garak has no ID and inactive" in stdout
    assert "Reporting for phase Transformer" in stdout
    assert "WARNING row: 5, message: 'At some point, Full name was added to the row_data and not declared a header'" in stdout
