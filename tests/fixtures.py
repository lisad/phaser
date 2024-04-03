import pytest
from phaser import Phase, row_step, check_unique, batch_step

@row_step
def null_step(row, **kwargs):
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
    """ Creates a subclass of Phase with one step, checking 'symbols' column for values from a list"""
    class Reconciler(Phase):
        MOCK_EXTERNAL_DATA = ['rabbit', 'pillow', 'clock', 'lintroller', 'bird', 'smokedetector']

        @row_step
        def check_known_symbols(row, **kwargs):
            if 'symbol' in row.keys():
                assert row['symbol'] in Reconciler.MOCK_EXTERNAL_DATA
            return row

        steps = [
            check_known_symbols
        ]

    return Reconciler
