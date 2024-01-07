import pytest
from pathlib import Path
from phaser import row_step, Phase, check_unique

@row_step
def null_step(phase, row):
    return row


@pytest.fixture
def test_data_phase_class(tmpdir):
    class TestData(Phase):
        steps = [check_unique('id')]

    return TestData


@pytest.fixture
def null_step_phase(tmpdir):
    """ This one is an instance """
    return Phase(name="do_nothing", steps=[null_step])


@pytest.fixture
def reconcile_phase_class(tmpdir):
    class Reconciler(Phase):
        MOCK_EXTERNAL_DATA = ['rabbit', 'pillow', 'clock', 'lintroller', 'bird', 'smokedetector']

        @row_step
        def check_known_symbols(self, row):
            if 'symbol' in row.keys():
                assert row['symbol'] in Reconciler.MOCK_EXTERNAL_DATA
            return row

        steps = [
            check_known_symbols
        ]

    return Reconciler
