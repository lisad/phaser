import pandas
import pandas as pd

from phaser import Phase, row_step, Pipeline, Column, IntColumn, read_csv
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path
from fixtures import reconcile_phase_class, test_data_phase_class, null_step_phase

current_path = Path(__file__).parent


def test_phase_load_data():
    phase = Phase()
    data = [{'id': 1, 'location': 'bridge'}, {'id': 2, 'location': 'engineering'}]
    phase.load_data(data)
    assert list(phase.headers) == ['id', 'location']
    assert phase.row_data == data

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

def test_save_only_some_columns():
    # Possibly this test would be more robust if we call 'run' and ensure that prepare_for_save is called
    # and drops columns before save, rather than explicitly call here.
    phase = Phase(name="transform",
                  columns=[Column(name="ID", save=True), Column(name="Status", save=False)])
    phase.load_data([{"ID": 1, "Status": "onboard"}])
    results = phase.run()
    assert "ID" in results[0].keys()
    assert "Status" not in results[0].keys()


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

    t = Transformer()
    input = read_csv(current_path / "fixture_files" / "crew.csv").to_dict('records')
    t.load_data(input)
    results = t.run()
    assert len(results) == len(input)

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
    transformer = Phase(steps=[full_name_step])

    transformer.load_data(read_csv(current_path / "fixture_files" / "crew.csv"))
    transformer.run_steps()
    assert "full name" in transformer.row_data[1]


@pytest.mark.skip("Pandas.read_csv doesn't allow this detection it just renames the 2nd 'name' to 'name.1'")
def test_duplicate_column_names(tmpdir):
    # See https://github.com/pandas-dev/pandas/issues/13262 - another reason to write our own CSV reader
    with open(tmpdir / 'dupe-column-name.csv', 'w') as f:
        f.write("id,name,name\n1,Percy,Jackson\n")
    # TODO - finish this test when we are able to - now that read_csv is a util that wraps
    # pandas.read_csv maybe we can look at the column names and look for duplicates without the #

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

def test_phase_saved_even_if_error(tmpdir):
