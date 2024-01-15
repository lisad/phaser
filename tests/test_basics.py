from phaser import Phase, row_step, Pipeline, Column
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path
from fixtures import reconcile_phase_class, test_data_phase_class, null_step_phase

current_path = Path(__file__).parent


def test_pipeline(tmpdir, null_step_phase, reconcile_phase_class):
    p = Pipeline(phases=[null_step_phase, reconcile_phase_class],
                 source=current_path / 'fixture_files' / 'crew.csv',
                 working_dir=tmpdir)
    p.run()


def test_pipeline_source_none(tmpdir, reconcile_phase_class):
    with pytest.raises(AssertionError):
        p = Pipeline(phases=[reconcile_phase_class], working_dir=tmpdir)
        p.run()


def test_load_and_save(tmpdir):
    source = current_path / "fixture_files" / "crew.csv"
    Phase().run(source, tmpdir / "Transformed-crew.csv")
    assert os.path.exists(os.path.join(tmpdir, "Transformed-crew.csv"))


def test_subclassing(tmpdir):
    class Transformer(Phase):
        pass

    source = current_path / "fixture_files" / "crew.csv"

    t = Transformer()
    t.run(source, tmpdir / "test_output.csv")
    assert os.path.exists(os.path.join(tmpdir, "test_output.csv"))


@row_step
def full_name_step(row, **kwargs):
    row["full name"] = " ".join([row["First name"], row["Last name"]])
    return row


def test_have_and_run_steps(tmpdir):
    source = current_path / "fixture_files" / "crew.csv"
    transformer = Phase(steps=[full_name_step])

    transformer.load(source)
    transformer.run_steps()
    assert "full name" in transformer.row_data[0]


@pytest.mark.skip("Pandas.read_csv doesn't allow this detection it just renames the 2nd 'name' to 'name.1'")
def test_duplicate_column_names(tmpdir):
    # See https://github.com/pandas-dev/pandas/issues/13262 - another reason to write our own CSV reader
    with open(tmpdir / 'dupe-column-name.csv', 'w') as f:
        f.write("id,name,name\n1,Percy,Jackson\n")
    phase = Phase()
    with pytest.raises(Exception):
        phase.load(tmpdir / 'dupe-column-name.csv')
        print(phase.row_data)

def test_do_column_stuff(tmpdir):
    source = current_path / "fixture_files" / "crew.csv"
    Phase(columns=[
            Column("First name"),
            Column("Last name")
        ]).run(source, tmpdir / "Transformed-employees-columns.csv")
    assert os.path.exists(os.path.join(tmpdir, "Transformed-employees-columns.csv"))
