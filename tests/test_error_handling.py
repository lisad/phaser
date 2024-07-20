from pathlib import Path

import pytest

from fixtures import reconcile_phase_class
from phaser import Phase, row_step, batch_step, WarningException, DropRowException, DataErrorException, \
    Pipeline, ON_ERROR_WARN, ON_ERROR_STOP_NOW

current_path = Path(__file__).parent


@row_step
def assert_false(row, context):
    assert False


@row_step
def check_deck_is_21(row, context):
    assert row['deck'] == '21'
    return row


@row_step
def drop_if_deck_is_not_21(row, context):
    if row['deck'] != '21':
        raise DropRowException("Deck # should be 21")
    return row

@row_step
def check_room_is_hologram_room(row, context):
    assert row['room'] == 'hologram'
    return row


@row_step
def warn_if_lower_decks(row, context):
    if int(row['deck']) < 10:
        raise WarningException("Lower decks should not be in this dataset")
    return row


@row_step
def warn_if_lower_decks_and_return_row(row, context):
    if int(row['deck']) < 10:
        context.add_warning('lower_decks_warning', row, "Lower decks should not be in this dataset")
        row['deck'] = 10
        row['incident'] = "Nothing happened"
    return row


@row_step
def has_lounge(row, context):
    return {**row, has_lounge: True}


def variance(array):
    mean = sum(array) / len(array)
    return sum((value - mean) ** 2 for value in array) / len(array)


@batch_step
def warn_tachyon_level_variance(batch, context):
    tachyon_values = [row['tachyon_level'] for row in batch]
    if variance(tachyon_values) > 10:
        raise WarningException("Tachyon variance at high levels")
    return batch


@batch_step
def error_tachyon_level_variance(batch, context):
    tachyon_values = [row['tachyon_level'] for row in batch]
    if variance(tachyon_values) > 10:
        raise Exception("Tachyon variance at high levels")


def test_error_provides_info(reconcile_phase_class):
    # An error should have the original index of the row, the row's values, the name of the step that failed,
    # and the exception class if not a PhaserException.
    phase = Phase(steps=[assert_false])
    phase.load_data([{'deck': '21'}])

    phase.run_steps()
    error = phase.context.get_events(phase=phase, row_num=1)[0]
    assert error['row'].row_num == 1
    assert error['row']['deck'] == '21'
    assert error['step_name'] == 'assert_false'
    assert error['message'] == "AssertionError raised (assert False)"


def test_row_processing_continues():
    """ After hitting an error, the importing should save that, skip that row on future tests,
    and report errors on any rows that have errors. """
    phase = Phase(steps=[check_deck_is_21, assert_false])
    phase.load_data([{'deck': '13'}, {'deck': '21'}])

    phase.run_steps()
    error2 = phase.context.get_events(phase=phase, row_num=2)[0]
    error1 = phase.context.get_events(phase=phase, row_num=1)[0]
    assert error1['step_name'] == 'check_deck_is_21'   # First row fails on the first step
    assert error2['step_name'] == 'assert_false'  # 2nd row fails on 2nd step


def test_row_processing_skips_row():
    """ After hitting an error on a row, future steps should not attempt to run on that row. In this data
    the first row would cause 2 errors but only the first should be listed.  """
    phase = Phase(steps=[check_deck_is_21, check_room_is_hologram_room])
    phase.load_data([{'deck': '13', 'room': 'lounge'}, {'deck': '21', 'room': 'hologram'}])

    phase.run_steps()
    the_error = phase.context.get_events(phase=phase, row_num=1)[0]
    assert the_error['step_name'] == 'check_deck_is_21'


def test_warning_contains_info():
    phase = Phase(steps=[warn_if_lower_decks])
    phase.load_data([{'deck': '21', 'incident': 'security'}, {'deck': '5', 'incident': 'security'}])

    phase.run_steps()
    the_warning = phase.context.get_events(phase=phase, row_num=2)[0]
    assert the_warning['row']['deck'] == '5'
    assert the_warning['step_name'] == 'warn_if_lower_decks'
    assert "Lower decks should not be in this dataset" in the_warning['message']


def test_warning_and_return_modified_row():
    phase = Phase(steps=[warn_if_lower_decks_and_return_row])
    phase.load_data([{'deck': '21', 'incident': 'security'}, {'deck': '5', 'incident': 'security'}])
    phase.run_steps()
    the_warning = phase.context.get_events(phase=phase, row_num=2)[0]
    assert the_warning['row']['deck'] == 10
    assert the_warning['step_name'] == 'lower_decks_warning'
    assert phase.row_data[1]['incident'] == "Nothing happened"


def test_drop_row_info():
    phase = Phase(steps=[drop_if_deck_is_not_21])
    phase.load_data([{'deck': '21'}, {'deck': '5'}])

    phase.run_steps()
    row_events = phase.context.get_events(phase=phase, row_num=2)
    assert row_events[0]['step_name'] == 'drop_if_deck_is_not_21'


def test_batch_step_error():
    phase = Phase(steps=[error_tachyon_level_variance])
    phase.load_data([{'tachyon_level': 513}, {'tachyon_level': 532}])
    phase.run_steps()
    row_events = phase.context.get_events(phase=phase, row_num='none')
    assert row_events[0]['step_name'] == 'error_tachyon_level_variance'


def test_batch_step_warning():
    phase = Phase(steps=[warn_tachyon_level_variance])
    phase.load_data([{'tachyon_level': 513}, {'tachyon_level': 532}])

    phase.run_steps()
    row_events = phase.context.get_events(phase=phase, row_num='none')
    assert row_events[0]['step_name'] == 'warn_tachyon_level_variance'
    assert row_events[0]['row'] is None


@pytest.mark.skip("Will test for error reporting format when we have output going to logger")
def test_row_error_formatting():
    phase = Phase(steps=[assert_false])
    phase.load_data([{'deck': '21'}])

    phase.run_steps()
    # phase.report_errors_and_warnings()  Errors and warning reporting moved to pipeline


@pytest.mark.skip("Not sure how we should deal with this - maybe leave in warning but remove in reporting")
def test_extra_fields_warn_once():
    # If we warn every time an extra field is added, this could be NxM warnings
    # we could store them differently to detect - although it is nice to know at least one row the warning
    # was added and in what step
    phase = Phase(steps=[has_lounge])
    phase.load_data([{'deck': 1}, {'deck': 2}])
    phase.run()
    assert len(phase.context.warnings) == 1


def test_row_step_in_renumbering_phase_can_report_row_num_error():
    @row_step
    def report_row_error(row, context):
        if row['floor'] == 13:
            raise DataErrorException("Floor cannot be #13")
        return row

    phase = Phase(steps=[report_row_error], renumber=True)
    phase.load_data([{'floor': 1}, {'floor': 13}])
    phase.run()
    row_events = phase.context.get_events(phase=phase, row_num=2)
    assert len(row_events) == 1
    assert 'Floor cannot' in row_events[0]['message']
    assert row_events[0]['row_num'] == 2


@batch_step
def report_row_error_in_batch(batch, context):
    raise DataErrorException("Look I just know there's a problem in row 2", row=batch[1])


def test_batch_step_can_return_row_num_in_error():
    phase = Phase(steps=[report_row_error_in_batch])
    phase.load_data([{'floor': 1}, {'floor': 13}])
    phase.run()
    error = phase.context.get_events(phase=phase, row_num=2)[0]
    assert 'just know' in error['message']
    assert error['type'] == 'ERROR'


def test_pipeline_error_policy(tmpdir):
    # WIth error policy Warning, the pipeline completes. Then with error policy STOP, the pipeline raises.
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[report_row_error_in_batch])],
                        error_policy=ON_ERROR_WARN)
    pipeline.run()
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[report_row_error_in_batch])],
                        error_policy=ON_ERROR_STOP_NOW)
    with pytest.raises(DataErrorException):
        pipeline.run()


def test_logging_respects_levels_and_handlers(tmpdir, caplog):
    import logging

    logger = logging.getLogger('phaser')

    log_file1 = tmpdir / "log_file1.txt"
    logger.addHandler(logging.FileHandler(log_file1))
    caplog.set_level(logging.INFO)
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[report_row_error_in_batch])],
                        error_policy=ON_ERROR_WARN)
    pipeline.run()
    log_file_data_with_info = open(log_file1, 'r').read()
    assert "Look I just know there's a problem" in log_file_data_with_info

    log_file2 = tmpdir / "log_file2.txt"
    logger.addHandler(logging.FileHandler(log_file2))
    caplog.set_level(logging.WARNING)
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[report_row_error_in_batch])],
                        error_policy=ON_ERROR_WARN)
    pipeline.run()
    log_file_data_with_warn = open(log_file2, 'r').read()
    assert "Look I just know there's a problem" in log_file_data_with_warn

    assert len(log_file_data_with_info) > len(log_file_data_with_warn)
