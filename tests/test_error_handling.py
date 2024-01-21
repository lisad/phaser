import pytest
from fixtures import reconcile_phase_class
from phaser import Pipeline, Phase, row_step, WarningException


@row_step
def assert_false(row, context):
    assert False


@row_step
def check_deck_is_21(row, context):
    assert row['deck'] == '21'
    return row


@row_step
def check_room_is_hologram_room(row, context):
    assert row['room'] == 'hologram'
    return row


@row_step
def warn_if_lower_decks(row, context):
    if int(row['deck']) < 10:
        raise WarningException("Lower decks should not be in this dataset")


def test_error_provides_info(reconcile_phase_class):
    # An error should have the original index of the row, the row's values, the name of the step that failed,
    # and the exception class if not a PhaserException.
    phase = Phase(steps=[assert_false])
    phase.load_data([{'deck': '21'}])

    phase.run_steps()
    error = phase.context.errors[0]
    assert error['row'][Pipeline.ROW_NUM_FIELD] == 0
    assert error['row']['deck'] == '21'
    assert error['step'] == 'assert_false'
    assert error['message'] == "AssertionError raised (assert False)"


def test_row_processing_continues():
    """ After hitting an error, the importing should save that, skip that row on future tests,
    and report errors on any rows that have errors. """
    phase = Phase(steps=[check_deck_is_21, assert_false])
    phase.load_data([{'deck': '13'}, {'deck': '21'}])

    phase.run_steps()
    assert len(phase.context.errors) == 2
    assert phase.context.errors[0]['step'] == 'check_deck_is_21'   # First row fails on the first step
    assert phase.context.errors[1]['step'] == 'assert_false'  # 2nd row fails on 2nd step


def test_row_processing_skips_row():
    """ After hitting an error on a row, future steps should not attempt to run on that row. In this data
    the first row would cause 2 errors but only the first should be listed.  """
    phase = Phase(steps=[check_deck_is_21, check_room_is_hologram_room])
    phase.load_data([{'deck': '13', 'room': 'lounge'}, {'deck': '21', 'room': 'hologram'}])

    phase.run_steps()
    assert len(phase.context.errors) == 1
    assert phase.context.errors[0]['step'] == 'check_deck_is_21'


def test_warning_contains_info():
    phase = Phase(steps=[warn_if_lower_decks])
    phase.load_data([{'deck': '21', 'incident': 'security'}, {'deck': '5', 'incident': 'security'}])

    phase.run_steps()
    assert len(phase.context.warnings) == 1
    the_warning = phase.context.warnings[1][0]
    assert the_warning['row']['deck'] == '5'
    assert the_warning['step'] == 'warn_if_lower_decks'
    assert the_warning['message'] == "Lower decks should not be in this dataset"


def test_drop_row_info():
    phase = Phase(steps=[check_deck_is_21], error_policy=Pipeline.ON_ERROR_DROP_ROW)
    phase.load_data([{'deck': '21'}, {'deck': '5'}])

    phase.run_steps()
    assert len(phase.context.dropped_rows) == 1
    assert phase.context.dropped_rows[1]['step'] == 'check_deck_is_21'
