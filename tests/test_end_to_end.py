import pytest
from pathlib import Path
from pipelines.employees import EmployeeReviewPipeline

current_path = Path(__file__).parent


def test_employee_pipeline(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    EmployeeReviewPipeline(source=source, working_dir=tmpdir).run()
