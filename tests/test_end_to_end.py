import pytest
import io
from contextlib import redirect_stdout
import os
from pathlib import Path
from pipelines.employees import EmployeeReviewPipeline
from phaser import read_csv, PHASER_ROW_NUM

current_path = Path(__file__).parent

@pytest.fixture
def pipeline(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    pipeline = EmployeeReviewPipeline(source=source, working_dir=tmpdir)
    pipeline.tmpdir = tmpdir
    return pipeline


def test_employee_pipeline(pipeline):
    pipeline.run()
    assert os.path.exists(pipeline.tmpdir / 'Validator_output_employees.csv')
    new_data = read_csv(pipeline.tmpdir / 'Transformer_output_employees.csv')
    assert len(new_data) == 3 # One employee should be dropped
    assert all([float(row['Bonus percent']) > 0.1 and float(row['Bonus percent']) < 0.2 for row in new_data])


def test_reporting(pipeline):
    f = io.StringIO()
    with redirect_stdout(f):
        # Having to grab stdout is probably temporary until we make pipeline more versatile in reporting errors
        pipeline.run()
    stdout = f.getvalue()
    assert "Reporting for phase Validator" in stdout
    assert "Employee Garak has no ID and inactive" in stdout
    assert "Reporting for phase Transformer" in stdout
    assert "'Full name' was added to the row_data and not declared a header'" in stdout
    # LMDTODO: Extra row counts should only be once, and this should be a unit test...
    assert stdout.count("'Full name' was added") == 1


def test_line_numbering(pipeline):
    pipeline.run()
    checkpoint = read_csv(pipeline.tmpdir / 'Validator_output_employees.csv')
    assert PHASER_ROW_NUM in checkpoint[0].keys()
    row_numbers = [row[PHASER_ROW_NUM] for row in checkpoint]
    assert row_numbers == ['1','2','4']

    new_data = read_csv(pipeline.tmpdir / 'Transformer_output_employees.csv')
    assert PHASER_ROW_NUM in new_data[0].keys()
    row_numbers = [row[PHASER_ROW_NUM] for row in new_data]
    assert row_numbers == ['1','2','4']


# LMDTODO Add a test that makes sure that if we load data in with numbers ALREADY IN IT, the
# numbers go up from there.  (i.e. the generator gets set to max value seen)