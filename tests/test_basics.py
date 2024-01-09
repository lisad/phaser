from phaser import Phase, row_step
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path

current_path = Path(__file__).parent


def test_load_and_save(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    Phase(source=source, working_dir=tmpdir, dest="Transformed-employees.csv").run()
    assert os.path.exists(os.path.join(tmpdir, "Transformed-employees.csv"))


@row_step
def full_name_step(phase, row):
    row["full name"] = " ".join([row["First name"], row["Last name"]])
    return row


def test_have_and_run_steps(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    transformer = Phase(source=source,
                        working_dir=tmpdir,
                        steps=[
                            full_name_step
                        ])

    transformer.load()
    transformer.run_steps()
    assert "full name" in transformer.row_data[0]

def test_duplicate_column_names(tmpdir):
    transformer = Phase(source='xyz', working_dir=tmpdir)
    # LMDTODO: Place duplicate column names in CSV and detect that this errors in load before
    # we get to saving row_data in a way that would hide this