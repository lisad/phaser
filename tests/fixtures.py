import pytest
from pathlib import Path
from phaser import row_step, Phase

@row_step
def null_step(phase, row):
    return row

@pytest.fixture
def transform_employees_phase(tmpdir):
    source = Path(__file__).parent / "fixture_files" / "employees.csv"
    transformer = Phase(source=source,
                        working_dir=tmpdir,
                        steps=[
                            null_step
                        ])
    return transformer
