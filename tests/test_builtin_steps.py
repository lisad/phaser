from pathlib import Path
import pytest
from phaser import (Phase, Pipeline, check_unique, read_csv, ON_ERROR_STOP_NOW, DataErrorException, IntColumn, sort_by,
                    filter_rows, flatten_column, flatten_all, drop_duplicate_rows)
from fixtures import test_data_phase_class
from test_csv import write_text

current_path = Path(__file__).parent


# Tests of the drop duplicates step
def test_drop_no_dupes():
    # If there are no duplicate rows, the resulting row data should be the same as the original data.
    data = [{'id': 1, 'val': 'left'}, {'id': 2, 'val': 'right'}]
    phase = Phase(name='phase', steps=[drop_duplicate_rows(None)])
    phase.load_data(data=data)
    phase.run_steps()
    assert phase.row_data == data


def test_drop_all_columns():
    # Using all columns to determine uniqueness, should result in 2 rows.
    phase = Phase(name='phase', steps=[drop_duplicate_rows(None)])
    phase.load_data(data=[{'id': 1, 'val': 'Samwell'}, {'id': 1, 'val': 'Wealwell'}, {'id': 1, 'val': 'Samwell'}])
    phase.run_steps()
    assert len(phase.row_data) == 2


def test_drop_one_column():
    # Using only one column 'id' to determine uniqueness, should result in 1 row.
    phase = Phase(name='phase', steps=[drop_duplicate_rows('id')])
    phase.load_data(data=[{'id': 1, 'val': 'Samwell'}, {'id': 1, 'val': 'Wealwell'}, {'id': 1, 'val': 'Samwell'}])
    phase.run_steps()
    assert len(phase.row_data) == 1

def test_drop_using_several_columns():
    # In this example, 'id' is not in the columns used to detect duplicates, so all rows are dropped but one.
    data = [{'id': aid, 'name': 'Barry', 'occupation': 'agent'} for aid in [1, 2, 3, 4, 5]]
    phase = Phase(name='phase', steps=[drop_duplicate_rows(['name', 'occupation'])])
    phase.load_data(data)
    phase.run_steps()
    assert len(phase.row_data) == 1

def test_drop_fn_provides_count():
    data = [{'id': aid, 'name': 'Barry', 'occupation': 'agent'} for aid in [1, 2, 3, 4, 5]]
    phase = Phase(name='phase', steps=[drop_duplicate_rows(['name', 'occupation'])])
    phase.load_data(data)
    phase.run_steps()
    events = phase.context.get_events(phase, 'none')
    assert events[0]['message'] == "4 rows dropped by drop_duplicate_rows(columns=['name', 'occupation'])"

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


def test_flatten_one_column_not_all():
    phase = Phase(name='phase', steps=[flatten_column('perf', deep=True)])
    phase.load_data(data=[{'employee_id': 123, 'perf': {'leadership': 3, 'communication': 2}, 'extra': {'foo': 'bar'}},
                          {'employee_id': 124, 'perf': {'leadership': 2, 'communication': 4}, 'extra': {'foo': 'baz'}}])
    phase.run_steps()
    assert phase.row_data[0]['perf__leadership'] > 2
    assert phase.row_data[0]['extra'] == {'foo': 'bar'}
    assert 'extra__foo' not in phase.row_data[0].keys()


NESTED_DATA_ROW = {
    'id': 123,
    'msg': {
        'type': {
            'oid': '1b2a',
            'name': 'Reply'
        },
        'content': 'Hello World'
    }
}

RESULT_ROW = {
    'id': 123,
    'msg__type__oid': '1b2a',
    'msg__type__name': 'Reply',
    'msg__content': 'Hello World'
}


def test_flatten_more_nested_value():
    phase = Phase(name='phase', steps=[flatten_column('msg')])
    phase.load_data(data=[NESTED_DATA_ROW])
    phase.run_steps()
    assert all([phase.row_data[0][key] == value for key, value in RESULT_ROW.items()])


def test_flatten_just_one_level():
    phase = Phase(name='phase', steps=[flatten_column('msg', deep=False)])
    phase.load_data(data=[NESTED_DATA_ROW])
    phase.run_steps()
    assert phase.row_data[0]['msg__content'] == "Hello World"
    assert 'msg__type__oid' not in phase.row_data[0].keys()
    assert phase.row_data[0]['msg__type'] == {'oid': '1b2a', 'name': 'Reply'}


def test_flatten_all():
    phase = Phase(name='phase', steps=[flatten_all])
    phase.load_data(data=[NESTED_DATA_ROW])
    phase.run_steps()
    assert all([phase.row_data[0][key] == value for key, value in RESULT_ROW.items()])


def test_flatten_value_none():
    phase = Phase(name='phase', steps=[flatten_column('perf')])
    phase.load_data(data=[{'employee_id': 123, 'perf': None, 'extra': {'foo': 'bar'}}])
    phase.run_steps()
    assert phase.row_data[0]['perf'] is None


def test_flatten_empty():
    phase = Phase(name='phase', steps=[flatten_column('perf')])
    phase.load_data(data=[{'employee_id': 123, 'perf': "", 'extra': {'foo': 'bar'}}])
    phase.run_steps()
    print(phase.row_data[0])
    assert phase.row_data[0]['perf'] == ""


def test_flatten_missing():
    # JSON data can easily be missing some fields in some records:
    phase = Phase(name='phase', steps=[flatten_column('perf')])
    phase.load_data(data=[{'employee_id': 123,  'extra': {'foo': 'bar'}}])
    phase.run_steps()
    assert 'perf' not in phase.row_data[0].keys()


def test_flatten_sometimes_a_dict():
    phase = Phase(name='phase', steps=[flatten_column('title')])
    phase.load_data(data=[{'id': 1, 'title': "Lions and Tigers"},
                          {'id': 2, 'title': {'en_US': 'Bears', 'fr_FR': 'Les ours'} }])
    phase.run_steps()
    assert phase.row_data[0]['title'] == "Lions and Tigers"
    assert phase.row_data[1]['title__en_US'] == "Bears"

