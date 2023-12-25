from phaser import Phase, row_step
import pytest
import os
from pathlib import Path

current_path = Path(__file__).parent

def test_load_and_save(tmpdir):
    source = current_path / "fixtures" / "employees.csv"
    Phase(source=source, working_dir=tmpdir, dest="Transformed-employees.csv").run()
    assert os.path.exists(os.path.join(tmpdir, "Transformed-employees.csv"))

@row_step
def full_name_step(phase, row):
    row["full name"] =  " ".join([row["First name"], row["Last name"]])
    return row

def test_have_and_run_steps(tmpdir):
    source = current_path / "fixtures" / "employees.csv"
    transformer = Phase(source=source,
                        working_dir=tmpdir,
                        steps = [
                            full_name_step
                        ])

    transformer.load()
    transformer.run_steps()
    assert "full name" in transformer.row_data[0]