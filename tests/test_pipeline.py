import os
from pathlib import Path
import pytest
from phaser import Pipeline, Phase, batch_step, PhaserError, DataException
from fixtures import reconcile_phase_class, null_step_phase

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


def test_number_go_up(tmpdir):
    @batch_step
    def adds_row(batch, context):
        batch.append({'key': "Fleet Victualling", 'value': 100})
        return batch

    # Also we should have a test that loads in __phaser_row_num__ values already set
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[adds_row])])
    pipeline.run()
    new_row = list(filter(lambda row: row['value'] == 100, pipeline.phases[0].row_data))[0]
    assert new_row.row_num > 5


def test_pipeline_can_accept_single_phase(tmpdir):
    # This instantiation call should not cause exceptions even though it passes an object to 'phases' not a list.
    Pipeline(working_dir=tmpdir, source='dontneed.csv', phases=Phase)


def test_error_report_in_phase_init(tmpdir):
    class PhaseWillErrorOnInit(Phase):
        def __init__(self):
            pass  # This will error during pipeline/phase init because the __init__ does not accept a context

    with pytest.raises(PhaserError) as excinfo:
        pipeline = Pipeline(working_dir=tmpdir, source='dontneed.csv', phases=PhaseWillErrorOnInit)
    assert "PhaseWillError" in excinfo.value.message


def test_error_report_in_pipeline_around_phase(tmpdir):
    class PhaseWillErrorOnRun(Phase):
        def run(self):
            {'id':3}['bogus']  # Cause a KeyError - checked manually that the KeyError is propagated not swallowed

    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[PhaseWillErrorOnRun(steps=[])])
    with pytest.raises(PhaserError) as excinfo:
        pipeline.run()
    assert "PhaseWillErrorOnRun" in excinfo.value.message


def test_phase_returns_no_rows(tmpdir):
    class PhaseReturnsNothing(Phase):
        def run(self):
            return []

    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[PhaseReturnsNothing(steps=[])])
    with pytest.raises(DataException) as exc_info:
        pipeline.run()
    assert "No rows left" in exc_info.value.message
