from phaser import Phase, row_step, Pipeline
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path
from fixtures import reconcile_phase_class, test_data_phase_class, null_step_phase

current_path = Path(__file__).parent


def test_pipeline(tmpdir, null_step_phase, reconcile_phase_class):
    p = Pipeline(phases=[null_step_phase, reconcile_phase_class],
                 source=current_path / 'fixture_files' / 'employees.csv',
                 working_dir=tmpdir)
    p.run()


def test_pipeline_source_none(tmpdir, reconcile_phase_class):
    with pytest.raises(AssertionError):
        p = Pipeline(phases=[reconcile_phase_class], working_dir=tmpdir)
        p.run()


def test_load_and_save(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    Phase().run(source, tmpdir / "Transformed-employees.csv")
    assert os.path.exists(os.path.join(tmpdir, "Transformed-employees.csv"))


def test_subclassing(tmpdir):
    class Transformer(Phase):
        pass

    source = current_path / "fixture_files" / "employees.csv"

    t = Transformer()
    t.run(source, tmpdir / "test_output.csv")
    assert os.path.exists(os.path.join(tmpdir, "test_output.csv"))


@pytest.mark.skip
def test_override_destination(tmpdir):
    pass


@row_step
def full_name_step(phase, row):
    row["full name"] = " ".join([row["First name"], row["Last name"]])
    return row


def test_have_and_run_steps(tmpdir):
    source = current_path / "fixture_files" / "employees.csv"
    transformer = Phase(steps=[full_name_step])

    transformer.load(source)
    transformer.run_steps()
    assert "full name" in transformer.row_data[0]


@pytest.mark.skip
def test_duplicate_column_names(tmpdir):
    transformer = Phase(source='xyz', working_dir=tmpdir)
    # LMDTODO: Place duplicate column names in CSV and detect that this errors in load before
    # we get to saving row_data in a way that would hide this
