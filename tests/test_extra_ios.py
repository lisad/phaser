import os
from pathlib import Path
import pytest
from phaser import Phase, context_step, Pipeline
from phaser.io import ExtraRecords, read_csv

current_path = Path(__file__).parent



@context_step
def add_an_output(context):
    context.set_output('doctors', ExtraRecords('doctors', data=[{'on_call': "Julian Bashir"}]))

class EmployeePhase(Phase):
    steps = [
        add_an_output
    ]

class MyPipeline(Pipeline):
    phases = [EmployeePhase]
    source = current_path / 'fixture_files' / 'crew.csv'


def test_extra_outputs(tmpdir):
    pipeline = MyPipeline(working_dir=tmpdir)
    pipeline.run()
    assert os.path.exists(tmpdir / 'doctors.csv')
    # We are checking for the full text of the file rather than using read_csv
    # to read the data back in and assert against it, because read_csv fails to
    # detect the csv dialect when there is only a single field (or maybe it is
    # when there is not enough data).
    text = (tmpdir / 'doctors.csv').read_text(encoding='utf8')
    assert text == 'on_call\nJulian Bashir\n'


@pytest.mark.skip("This doesn't work yet - the pipeline tells the context when to save extra outputs")
def test_extra_outputs_without_phase(tmpdir):
    phase = EmployeePhase()
    phase.run(current_path / "fixture_files" / "crew.csv", tmpdir)
    assert os.path.exists(tmpdir / 'employee_phase_managers.csv')
