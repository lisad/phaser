import pandas
import pandas as pd

from phaser import Phase, row_step, Pipeline, Column, IntColumn
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path
from fixtures import reconcile_phase_class, test_data_phase_class, null_step_phase

current_path = Path(__file__).parent


def test_pipeline(tmpdir, null_step_phase, reconcile_phase_class):
    # This pipeline should run two phases (one an instance, one a class) and have both outputs
    p = Pipeline(phases=[null_step_phase, reconcile_phase_class],
                 source=current_path / 'fixture_files' / 'crew.csv',
                 working_dir=tmpdir)
    p.run()
    assert os.path.exists(os.path.join(tmpdir, 'do_nothing_output_crew.csv'))
    assert os.path.exists(os.path.join(tmpdir, 'Reconciler_output_crew.csv'))


def test_pipeline_source_none(tmpdir, reconcile_phase_class):
    with pytest.raises(AssertionError):
        p = Pipeline(phases=[reconcile_phase_class], working_dir=tmpdir)
        p.run()


def test_load_and_save(tmpdir):
    source = current_path / "fixture_files" / "crew.csv"
    dest = os.path.join(tmpdir, "Transformed-crew.csv")
    Phase().run(source, dest)
    assert os.path.exists(dest)
    with open(dest) as f:
        first_line = f.readline()
    assert first_line.startswith("First name,")
    assert first_line.endswith(",pay per\n")


def test_save_only_some_columns(tmpdir):
    # Possibly this test would be more robust if we call 'run' and ensure that prepare_for_save is called
    # and drops columns before save, rather than explicitly call here.
    phase = Phase(name="transform",
                  columns=[Column(name="ID", save=True), Column(name="Status", save=False)])
    phase.load_data([{"ID": 1, "Status": "onboard"}])
    phase.prepare_for_save()
    phase.save(tmpdir / "test_drop_column.csv")
    with open(tmpdir / "test_drop_column.csv") as f:
        first_line = f.readline()
        assert first_line == "ID\n"


def test_drop_col_works_if_not_exist(tmpdir):
    # Status column isn't in the data, yet it doesn't cause the save=False logic to have a KeyError.
    phase = Phase(name="transform",
                  columns=[Column(name="ID", save=True), Column(name="Status", save=False)])
    phase.load_data([{"ID": 1, "Location": "onboard"}])
    phase.prepare_for_save()
    assert 'Status' not in pd.DataFrame(phase.row_data).columns


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


# Phase tests

def phase_accepts_single_col():
    # Helpfully should wrap column in a list if only one is defined
    col = Column(name="Test")
    phase = Phase(name="Transform", columns=col)
    assert phase.columns == [col]


def test_have_and_run_steps(tmpdir):
    source = current_path / "fixture_files" / "crew.csv"
    transformer = Phase(steps=[full_name_step])

    transformer.load(source)
    transformer.run_steps()
    assert "full name" in transformer.row_data[1]


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


def test_column_error_drops_row():
    col = IntColumn(name='level', min_value=0, on_error='drop_row')
    phase = Phase("test", columns=[col])
    phase.load_data([{'level': -1}])
    phase.do_column_stuff()
    assert phase.row_data == []


def test_column_error_adds_warning():
    col = IntColumn(name='level', min_value=0, on_error='warn')
    phase = Phase("test", columns=[col])
    phase.load_data([{'level': -1}])
    phase.do_column_stuff()
    warnings_for_row = list(phase.context.warnings.values())[0]
    assert len(warnings_for_row) == 1
    assert "level" in warnings_for_row[0]['message']
    assert warnings_for_row[0]['step'] == 'cast_each_column_value'


@pytest.mark.skip("User can write steps that violate the column contracts, and save the output. we should fix that.")
def test_drop_field_during_step(tmpdir):
    @row_step
    def drop_step(the_row, **kwargs):
        if the_row["crew id"] == 1:
            del the_row["crew id"]
        return the_row

    col = IntColumn(name='crew id', min_value=1)
    phase = Phase('test', columns=[col], steps=[drop_step])
    phase.run(source=current_path / 'fixture_files' / 'crew.csv',
              destination=tmpdir / 'tmp.csv')
    output = pandas.read_csv(tmpdir / 'tmp.csv').to_dict('records')
    for row in output:
        assert row['crew id'] in ["1", "2"]
