import numpy
import pandas
import pytest
import phaser
from phaser.differ import DiffingPhase, diff_row, diff_rows, is_not_equal

@pytest.mark.parametrize("a, b, expected",
    [
        ({}, {}, {}),
        # Change all fields
        ({'name': 'Kirk', 'crew id': 1},
         {'name': 'McCoy', 'crew id': 2},
         {'name': ('Kirk', 'McCoy'), 'crew id': (1, 2)}),
        # Change no fields
        ({'name': 'Kirk', 'crew id': 1},
         {'name': 'Kirk', 'crew id': 1},
         {}),
        # Change a single field
        ({'name': 'Kirk', 'crew id': 1},
         {'name': 'Kirk', 'crew id': 10},
         {'crew id': (1, 10)}),
        # Add a field
        ({'name': 'Kirk', 'crew id': 1},
         {'name': 'Kirk', 'crew id': 1, 'rank': 'Captain'},
         {'rank': (None, 'Captain')}),
        # Remove a field
        ({'name': 'Kirk', 'crew id': 1, 'rank': 'Captain'},
         {'name': 'Kirk', 'crew id': 1},
         {'rank': ('Captain', None)}),
        # Treat NA as equal to NA, even though Pandas does not
        ({'name': pandas.NA},
         {'name': pandas.NA},
         {}),
        # Treat nan as equal to nan, even though numpy does not
        ({'name': numpy.nan},
         {'name': numpy.nan},
         {}),

    ]
)
def test_diff_row(a, b, expected):
    diff = diff_row(a, b)
    assert diff == expected

@pytest.mark.skip
@pytest.mark.parametrize("aa, bb, expected",
    [
        ([], [], []),
        ([1], [2], [1, 2]),
    ]
)
def test_diff_rows(aa, bb, expected):
    diff = diff_rows(aa, bb)
    assert diff == expected

@pytest.mark.parametrize("a, b, expected",
    [
        (pandas.NA, pandas.NA, False),
        (numpy.nan, numpy.nan, False),
        (pandas.NA, numpy.nan, False),
        (numpy.nan, pandas.NA, False),
        ('a', pandas.NA, True),
        (pandas.NA, 'b', True),
        ('a', numpy.nan, True),
        (numpy.nan, 'b', True),
        ('a', 'b', True),
        (['a'], ['a'], False),
    ]
)
def test_not_equal(a, b, expected):
    assert is_not_equal(a, b) == expected

def test_diffing_phase(tmpdir):
    @phaser.row_step
    def null_step(row, **kwargs):
        return row

    @phaser.row_step
    def change_step(row, **kwargs):
        new_row = {
            'name': row['name'] + '-change',
            'age': row['age'] + 20,
        }
        return new_row

    data = [
        {'name': 'Wonderkid', 'age': 16},
        {'name': 'Wonderbaby', 'age': 2},
    ]
    phase = phaser.Phase(
        name="inner phase",
        steps=[
            null_step,
            change_step,
        ],
    )
    diffing_phase = DiffingPhase(phase)
    diffing_phase.load_data(data)
    diffing_phase.run_steps()
    assert diffing_phase.row_data == [
        {'name': 'Wonderkid-change', 'age': 36},
        {'name': 'Wonderbaby-change', 'age': 22},
    ]
