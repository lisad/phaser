import logging
import os
from pathlib import Path
import pytest
from phaser import Pipeline, Phase, batch_step, PhaserError, DataException, ON_ERROR_STOP_NOW
from fixtures import reconcile_phase_class, null_step_phase

from test_csv import write_text

current_path = Path(__file__).parent


def test_pipeline(tmpdir, null_step_phase, reconcile_phase_class):
    # This pipeline should run two phases (one an instance, one a class) and have both outputs
    p = Pipeline(phases=[null_step_phase, reconcile_phase_class],
                 source=current_path / 'fixture_files' / 'crew.csv',
                 working_dir=tmpdir)
    p.run()
    assert os.path.exists(os.path.join(tmpdir, 'do_nothing_output.csv'))
    assert os.path.exists(os.path.join(tmpdir, 'Reconciler_output.csv'))


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


def test_pipeline_logging(tmpdir, null_step_phase, caplog):
    with caplog.at_level(logging.INFO):
        p = Pipeline(phases=[null_step_phase],
                     source=current_path / 'fixture_files' / 'crew.csv',
                     working_dir=tmpdir)
        p.run()

    log_messages = [record.message for record in caplog.records]
    assert any("Loading input from" in message for message in log_messages)
    assert any("saved output to" in message for message in log_messages)


def test_pipeline_error_handling_logging(tmpdir, caplog):
    class DummyPhase(Phase):
        def run(self):
            raise PhaserError("Error in pipeline running")

    with caplog.at_level(logging.WARNING):
        pipeline = Pipeline(phases=[DummyPhase()],
                            source=current_path / 'fixture_files' / 'departments.csv',
                            working_dir=tmpdir,
                            error_policy=ON_ERROR_STOP_NOW)
        with pytest.raises(PhaserError):
            pipeline.run()
            log_messages = [record.message for record in caplog.records]
            assert any("Error in pipeline running" in message for message in log_messages)


def test_pipeline_wont_overwrite_source(tmpdir, null_step_phase):
    # First add the source file to the tmpdir so it WOULD be overwritten
    with pytest.raises(PhaserError):
        Pipeline(phases=[null_step_phase],
                 source=tmpdir / 'source_copy.csv',
                 working_dir=tmpdir)

    with pytest.raises(PhaserError):
        Pipeline(phases=[null_step_phase],
                 source=tmpdir / 'do_nothing_output.csv',
                 working_dir=tmpdir)

    # If the source file is named one of the outputs but in a different directory, it should be OK
    Pipeline(phases=[null_step_phase],
             source=tmpdir / 'do_nothing_output.csv',
             working_dir=tmpdir.mkdir("subdir"))
