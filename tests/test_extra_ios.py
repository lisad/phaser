import os
from pathlib import Path
import pytest
from phaser import Phase, context_step, Pipeline

current_path = Path(__file__).parent



@context_step
def add_an_output(context):
    context.add_output('doctors', [{'on_call': "Julian Bashir"}])

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


@pytest.mark.skip("This doesn't work yet - the pipeline tells the context when to save extra outputs")
def test_extra_outputs_without_phase(tmpdir):
    phase = EmployeePhase()
    phase.run(current_path / "fixture_files" / "crew.csv", tmpdir)
    assert os.path.exists(tmpdir / 'employee_phase_managers.csv')
