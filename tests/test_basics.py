from phaser import Phase, row_step, Pipeline, Column, IntColumn, read_csv, DataException
import pytest  # noqa # pylint: disable=unused-import
import os
from pathlib import Path
from fixtures import reconcile_phase_class, null_step_phase

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


def test_return_only_some_columns():
    # If a column is marked for NOT saving, it shouldn't be returned to the Pipeline to save.
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
    assert 'Status' not in phase.row_data[0].keys()

# The prepare_for_save function used to use a DataFrame to succintcly drop the
# columns, but DataFrame changes types sometimes. In particular, if there was
# a column of `int`s that had some `None` values in it, DataFrame converted that
# column to a `float`. Not what we want. This function checks that that behavior
# no longer occurs.
def test_dropping_columns_does_not_change_type(tmpdir):
    phase = Phase(name="transform",
                  columns=[IntColumn(name="id")])
    phase.load_data([{"id": 1}, {"id": None}, {"id": 2}])
    phase.prepare_for_save()
    assert phase.row_data.to_records() == [{"id": 1}, {"id": None}, {"id": 2}]

def test_subclassing(tmpdir):
    class Transformer(Phase):
        pass

    t = Transformer()
    data = read_csv(current_path / "fixture_files" / "crew.csv")
    t.load_data(data)
    results = t.run()
    assert len(results) == len(data)


@row_step
def full_name_step(row, **kwargs):
    row["full name"] = " ".join([row["First name"], row["Last name"]])
    return row


# Phase tests

def test_phase_accepts_single_col():
    # Helpfully should wrap column in a list if only one is defined
    col = Column(name="Test")
    phase = Phase(name="Transform", columns=col)
    assert phase.columns == [col]


def test_have_and_run_steps(tmpdir):
    transformer = Phase(steps=[full_name_step])

    transformer.load_data(read_csv(current_path / "fixture_files" / "crew.csv"))
    transformer.run_steps()
    assert "full name" in transformer.row_data[1]


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
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv').to_dict('records'))
    output = phase.run()
    for row in output:
        assert row['crew id'] in ["1", "2"]


def test_phase_saved_even_if_error(tmpdir):
    col = IntColumn(name='level', min_value=0)
    phase = Phase("test", columns=[col])
    with open(tmpdir / 'negative-level.csv', 'w') as f:
        f.write('crew member,level\n"B\'Elanna Torres",-1\n')
    pipeline = Pipeline(tmpdir, tmpdir / 'negative-level.csv', phases=[phase])
    pipeline.setup_phases()
    with pytest.raises(DataException):
        pipeline.run_phase(phase, tmpdir / 'negative-level.csv', tmpdir / 'test-saved-despite-error.csv')
    assert os.path.exists(os.path.join(tmpdir, 'test-saved-despite-error.csv'))
