from pathlib import Path
import pytest
from phaser import Phase, check_unique, read_csv, ON_ERROR_STOP_NOW, DataErrorException, IntColumn, sort_by, filter_rows
from fixtures import test_data_phase_class

current_path = Path(__file__).parent

# Tests of the check_unique step


def test_check_unique_works():
    phase = Phase(steps=[check_unique('crew id')])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv'))
    phase.run_steps()


def test_check_unique_fails(test_data_phase_class):
    phase = test_data_phase_class()
    phase.context.error_policy = ON_ERROR_STOP_NOW
    phase.load_data([{'id': '1'}, {'id': '1'}])
    with pytest.raises(DataErrorException):
        phase.run_steps()


def test_check_unique_strips_spaces():
    fn = check_unique('id')
    with pytest.raises(DataErrorException):
        fn([{'id': " 1 "}, {'id': '1'}])


def test_check_unique_without_stripping():
    fn = check_unique('name', strip=False)
    fn([{'name': '  Sam'}, {'name': 'Sam'}])


def test_check_unique_case_sensitive():
    fn = check_unique('dept')
    fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "eng"}, {'dept': None}])


def test_check_unique_case_insensitive():
    fn = check_unique('dept', ignore_case=True)
    with pytest.raises(DataErrorException):
        fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "Eng"}])


def test_check_unique_null_values():
    fn = check_unique('id')
    fn([{'id': "1"}, {'id': "2"}, {'id': None}])


def test_check_unique_int_values():
    fn = check_unique('id')
    fn([{'id': 1}, {'id': 2}, {'id': 3}])   # passes
    with pytest.raises(DataErrorException):
        fn([{'id': 1}, {'id': 2}, {'id': 2}])


def test_check_unique_takes_column_instance():
    col = IntColumn(name='id')
    fn = check_unique(col)
    fn([{'id': 1}, {'id': 2}, {'id': 3}])


def test_check_unique_is_helpful_when_column_missing():
    fn = check_unique('crew id')
    with pytest.raises(DataErrorException) as error_info:
        fn([{'id': 1}, {'id': 2}, {'id': 3}])
    assert "Check_unique: Some or all rows did not have 'crew id' present" in error_info.value.message


# Testing builtin sort_by step


def test_sort_with_col_name():
    phase = Phase(steps=[sort_by("id")])
    phase.load_data([{'id': 1}, {'id': 3}, {'id': 2}, {'id': 0}])
    phase.run_steps()
    assert all([phase.row_data[i]['id'] == i for i in range(4)])


def test_sort_with_col_obj():
    id_col = IntColumn(name='id')
    phase = Phase(columns=[id_col], steps=[sort_by(id_col)])
    phase.load_data([{'id': 1}, {'id': 3}, {'id': 2}, {'id': 0}])
    phase.run_steps()
    assert all([phase.row_data[i]['id'] == i for i in range(4)])


# Testing filter_rows step


def test_filter_rows():
    phase = Phase(name='foo', steps=[filter_rows(lambda row: row['rank'] == "Doctor")])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv'))
    phase.run_steps()
    assert all([row['rank'] == "Doctor" for row in phase.row_data])
    assert phase.context.phase_has_errors('foo') is False
    assert len(phase.context.events['foo']) == 1  # No rows should have warnings besides the 'none' row


def test_filter_rows_doesnt_warn_extra():
    phase = Phase(name='foo', steps=[filter_rows(lambda row: row['rank'] == "Doctor")])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv'))
    phase.run_steps()
    assert len(phase.context.events['foo']) == 1  # No rows should have warnings besides the 'none' row
    assert len(phase.context.get_events(phase, 'none')) == 1


def test_filter_rows_message():
    def find_doctors(row):
        return row['rank'] == "Doctor"

    phase = Phase(name='foo', steps=[filter_rows(find_doctors)])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv'))
    phase.run_steps()
    assert all([row['rank'] == "Doctor" for row in phase.row_data])
    events = phase.context.get_events(phase, 'none')
    assert events[0]['message'] == "1 rows dropped in filter_rows with 'find_doctors'"
