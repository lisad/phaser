from pathlib import Path
import pytest

from phaser import (check_unique, Phase, row_step, batch_step, context_step, Pipeline, sort_by, IntColumn,
                    DataErrorException, DropRowException, PhaserError, read_csv, dataframe_step,
                    PHASER_ROW_NUM, ON_ERROR_STOP_NOW)
import phaser
from fixtures import test_data_phase_class
from steps import sum_bonuses

current_path = Path(__file__).parent


# Tests of the operation of steps

# Row steps

def test_row_step_returns_new_dict():
    @row_step
    def my_row_step(row, context):
        # By not returning the row but a dict with its values, convert the Record object into a simple dict
        return {row.keys[0]: row.values()[0]}

    phase = Phase(steps=[my_row_step])
    phase.load_data([{'grade': 'A'}, {'grade': 'B'}])
    phase.run_steps()
    assert phase.row_data[0].row_num == 1
    assert phase.row_data[0]['grade'] == 'A'

# Batch steps


def test_simple_batch_step():
    @batch_step
    def sum_so_far(batch, context):
        running_sum = 0
        for row in batch:
            running_sum = running_sum + row['value']
            row['sum'] = running_sum
        return batch

    phase = Phase(steps=[sum_so_far])
    phase.load_data([{'value': 1}, {'value': 2}])
    phase.run_steps()
    assert phase.row_data == [{'value': 1, 'sum': 1}, {'value': 2, 'sum': 3}]


def test_batch_step_cant_use_drop_row_exception():
    @batch_step
    def try_something_nonsensical(batch, context):
        raise DropRowException("How could this even work")

    phase = Phase(steps=[try_something_nonsensical])
    phase.load_data([{'a': 'b'}])
    with pytest.raises(PhaserError) as e:
        phase.run_steps()


def test_batch_step_missing_param():
    @batch_step
    def simple_step(batch):
        batch[0]['a'] = 'c'
        return batch

    @batch_step
    def step_with_context(batch, context):
        context.add_variable('been here', True)
        return batch

    phase = Phase(steps=[simple_step, step_with_context])
    phase.load_data([{'a': 'b'}])
    phase.run_steps()
    assert phase.row_data == [{'a': 'c'}]


#@pytest.mark.skip("Needs to be fixed - when we add a row to a Records instance it should have a row number")
def test_batch_step_can_add_row():
    @batch_step
    def add_row(batch, context):
        batch.append({'deck': 5, 'location': 'secret lounge'})
        return batch

    phase = Phase(steps=[add_row])
    phase.load_data([{'deck': 10, 'location': '10 Forward'}])
    phase.run_steps()
    assert phase.row_data[0].row_num == 1
    assert phase.row_data[1]['deck'] == 5
    assert phase.row_data[1].row_num == 2


def test_context_available_to_step():
    @row_step
    def replace_value_fm_context(row, context):
        row['secret'] = context.get('secret')
        return row


@row_step
def replace_value_fm_context(row, context):
    row['secret'] = context.get('secret')
    return row


def test_context_available_to_step():
    transformer = Phase(steps=[replace_value_fm_context])
    transformer.context.add_variable('secret', "I'm always angry")
    transformer.load_data([{'id': 1, 'secret': 'unknown'}])
    transformer.run_steps()
    assert transformer.row_data[0]['secret'] == "I'm always angry"

# Tests of the check_unique step


def test_builtin_step():
    phase = Phase(steps=[check_unique('crew id')])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'crew.csv'))
    phase.run_steps()


def test_check_unique_fails(test_data_phase_class):
    phase = test_data_phase_class(error_policy=ON_ERROR_STOP_NOW)
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


# Testing context steps


def test_context_step():
    @context_step
    def use_context(context):
        context.add_variable("ship_name", "USS Enterprise")

    phase = Phase(steps=[use_context])
    phase.load_data([{}])
    phase.run_steps()
    assert phase.context.get("ship_name") == "USS Enterprise"


def test_context_step_cant_raise_drop_row():
    @context_step
    def raise_inappropriate_exception(context):
        raise DropRowException("Can we drop a row here? No we cannot.")

    phase = Phase(steps=[raise_inappropriate_exception])
    phase.load_data([{}])
    with pytest.raises(PhaserError) as exc_info:
        phase.run_steps()
    assert "DropRowException can't" in exc_info.value.message


def test_context_step_cant_return_random_stuff():
    @context_step
    def return_inappropriate_stuff(context):
        return {'message': 'what is the pipeline even supposed to do with this'}

    phase = Phase(steps=[return_inappropriate_stuff])
    phase.load_data([{}])
    with pytest.raises(PhaserError) as exc_info:
        phase.run_steps()
    assert "return a value" in exc_info.value.message


def test_context_step_keeps_numbers():
    @context_step
    def a_step(context):
        assert 1 > 0

    row_num_gen = phaser.records.row_num_generator()
    # PIpeline will set up Records that converts __phaser_row_num__ into record.row_num.  Replicate that setup
    # so we can see that it keeps that setup.
    data_with_row_numbers = [
        {'id': 3, 'age': 48, PHASER_ROW_NUM: 3}
    ]
    data = phaser.records.Records(data_with_row_numbers, row_num_gen)
    phase = Phase("test", steps=[a_step])
    phase.load_data(data)
    # Before running, the 2nd row, index 1, should keep the # 3... and after running also
    assert phase.row_data[0].row_num == 3
    phase.run()
    assert phase.row_data[0].row_num == 3


# testing dataframe steps


def test_dataframe_step():
    phase = Phase(steps=[sum_bonuses])
    phase.load_data([{'eid': '001', 'commission': 1000, 'performance': 9000},
                     {'eid': '002', 'commission': 9000, 'performance': 1000}])
    phase.run_steps()
    assert all([row['total'] == 10000 for row in phase.row_data])


def test_dataframe_step_doesnt_declare_context():
    @dataframe_step(pass_row_nums=False)
    def sum_bonuses_one_param(df):
        df['total'] = df.sum(axis=1, numeric_only=True)
        return df

    phase = Phase(steps=[sum_bonuses_one_param])
    phase.load_data([{'eid': '001', 'commission': 1000, 'performance': 9000}])
    phase.run_steps()
    assert phase.row_data[0]['total'] == 10000


def test_dataframe_step_doesnt_return_df():
    @dataframe_step
    def sum_bonuses_forgot_return(df):
        df['total'] = df.sum(axis=1, numeric_only=True)

    phase = Phase(steps=[sum_bonuses_forgot_return])
    phase.load_data([{'eid': '001', 'commission': 1000, 'performance': 9000}])
    with pytest.raises(PhaserError) as e:
        phase.run_steps()
    assert 'pandas DataFrame' in e.value.message


def test_multiple_step_types():
    phase = Phase(steps=[sum_bonuses, replace_value_fm_context])
    phase.load_data([{'eid': '001', 'commission': 1000, 'performance': 9000}])
    phase.run_steps()
    assert set(phase.row_data[0].keys()) == set(['eid', 'commission', 'performance', 'total', 'secret'])

