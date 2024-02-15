import pytest
from pathlib import Path
from pipelines.employees import EmployeeReviewPipeline

current_path = Path(__file__).parent


def test_employee_pipeline(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    EmployeeReviewPipeline(source=source, working_dir=tmpdir).run()


@pytest.mark.skip("work in progress")
def test_employee_pipeline_calculates_bonus():
    assert False
    # LMDTODO: Make sure bonuses are calculated correctly! etc!


    # ALso add tests that introduce errors to employee data & run pipeline or phases?