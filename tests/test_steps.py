from pathlib import Path

import pandas
import pytest

from phaser import (Phase, row_step, batch_step, context_step,
                    DropRowException, PhaserError, dataframe_step,
                    PHASER_ROW_NUM, ON_ERROR_STOP_NOW)
from phaser.io import ExtraMapping
import phaser
from steps import sum_bonuses

current_path = Path(__file__).parent


# Tests of the operation of step decorators and internal step functionality

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


@row_step(extra_sources=['extra'])
def append_extra_to_row(row, extra):
    row['extra'] = extra[row['number']]
    return row


EXTRA_DATA = ExtraMapping('extra', {12: 'A dozen', 13: "Baker's dozen"})


def test_extra_sources_to_row_step():
    phase = Phase(
        steps=[append_extra_to_row],
        extra_sources=[EXTRA_DATA],
    )
    phase.context.set_source('extra', EXTRA_DATA)
    phase.load_data([{'number': 12}, {'number': 13}])
    phase.run_steps()
    assert phase.row_data == [
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ]


def test_extra_sources_but_no_context():
    # When the user tries to test a step like _append_extra_to_row_ and forgets to add a context, we should
    # provide a helpful error.
    with pytest.raises(PhaserError) as exc_info:
        append_extra_to_row({'original': 'data'}, EXTRA_DATA)
    assert "without a context" in exc_info.value.message


@row_step(extra_outputs=['extra'])
def collect_extra_from_row(row, extra):
    extra[row['number']] = row['extra']
    return row


def test_extra_outputs_to_row_step():
    extra = ExtraMapping('extra', {})
    phase = Phase(
        steps=[collect_extra_from_row],
        extra_outputs=[extra],
    )
    phase.load_data([
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ])
    phase.run_steps()
    # This is a hack that depends on knowing that outputs are available as
    # sources in the context.
    assert phase.context.get_source('extra') == {
        12: 'A dozen',
        13: "Baker's dozen",
    }


def test_extra_outputs_but_no_context():
    # When the user tries to test a step like _collect_extra_from_row_ and forgets to pass a context to add
    # extra outputs to, we need to test for that and return a helpful error.
    with pytest.raises(PhaserError) as exc_info:
        collect_extra_from_row({'id': 1, 'what': 'the row'}, ExtraMapping('extra', {}))
    assert "without a context" in exc_info.value.message


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


def test_extra_sources_to_batch_step():
    @batch_step(extra_sources=['extra'])
    def append_extra_to_batch(batch, extra):
        for row in batch:
            row['extra'] = extra[row['number']]
        return batch

    extra = ExtraMapping('extra', {12: 'A dozen', 13: "Baker's dozen"})
    phase = Phase(
        steps=[append_extra_to_batch],
        extra_sources=[extra],
    )
    phase.context.set_source('extra', extra)
    phase.load_data([{'number': 12}, {'number': 13}])
    phase.run_steps()
    assert phase.row_data == [
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ]

def test_extra_outputs_to_batch_step():
    @batch_step(extra_outputs=['extra'])
    def collect_extra_from_batch(batch, extra):
        for row in batch:
            extra[row['number']] = row['extra']
        return batch

    extra = ExtraMapping('extra', {})
    phase = Phase(
        steps=[collect_extra_from_batch],
        extra_outputs=[extra]
    )
    phase.context.error_policy=ON_ERROR_STOP_NOW
    phase.load_data([
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ])
    phase.run_steps()
    # This is a hack that depends on knowing that outputs are available as
    # sources in the context.
    assert phase.context.get_source('extra') == {
        12: 'A dozen',
        13: "Baker's dozen",
    }


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

def test_extra_sources_to_context_step():
    @context_step(extra_sources=['extra'])
    def append_extra_to_context(context, extra):
        sink = context.get('sink')
        for k, v in extra.items():
            sink[k] = v

    extra = ExtraMapping('extra', {12: 'A dozen', 13: "Baker's dozen"})
    phase = Phase(
        steps=[append_extra_to_context],
        extra_sources=[extra],
    )
    phase.context.set_source('extra', extra)
    phase.context.add_variable('sink', {})
    phase.load_data([{'number': 12}, {'number': 13}])
    phase.run_steps()
    assert phase.context.get('sink') == {12: 'A dozen', 13: "Baker's dozen"}

def test_extra_outputs_from_context_step():
    @context_step(extra_outputs=['extra'])
    def collect_extra_from_context(context, extra):
        extra[12] = 'A dozen'
        extra[13] = "Baker's dozen"

    extra = ExtraMapping('extra', {})
    phase = Phase(
        steps=[collect_extra_from_context],
        extra_outputs=[extra]
    )
    phase.context.error_policy=ON_ERROR_STOP_NOW
    phase.load_data([
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ])
    phase.run_steps()
    # This is a hack that depends on knowing that outputs are available as
    # sources in the context.
    assert phase.context.get_source('extra') == {
        12: 'A dozen',
        13: "Baker's dozen",
    }

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

def test_extra_sources_to_df_step():
    @dataframe_step(extra_sources=['extra'])
    def append_extra_to_df(df, extra):
        df['extra'] = df['number'].map(lambda x: extra[x])
        return df

    extra = ExtraMapping('extra', {12: 'A dozen', 13: "Baker's dozen"})
    phase = Phase(
        steps=[append_extra_to_df],
        extra_sources=[extra],
    )
    phase.context.set_source('extra', extra)
    phase.load_data([{'number': 12}, {'number': 13}])
    phase.run_steps()
    assert phase.row_data == [
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ]

def test_extra_outputs_from_df_step():
    @dataframe_step(extra_outputs=['extra'])
    def collect_extra_from_df(df, extra):
        for row, num in enumerate(df['number'].values):
            extra[num] = df['extra'].at[row]
        return df

    extra = ExtraMapping('extra', {})
    phase = Phase(
        steps=[collect_extra_from_df],
        extra_outputs=[extra]
    )
    phase.context.error_policy=ON_ERROR_STOP_NOW
    phase.load_data([
        {'number': 12, 'extra': 'A dozen'},
        {'number': 13, 'extra': "Baker's dozen"},
    ])
    phase.run_steps()
    # This is a hack that depends on knowing that outputs are available as
    # sources in the context.
    assert phase.context.get_source('extra') == {
        12: 'A dozen',
        13: "Baker's dozen",
    }


def test_dataframe_step_skip_check_size():
    @dataframe_step(check_size=False)
    def double_rows(df):
        return pandas.concat([df, df.copy()], ignore_index=True, sort=False)

    phase = Phase(steps=[double_rows])
    phase.load_data([{'id': 1, 'val': 10}])
    phase.run_steps()
    assert len(phase.row_data) == 2
    assert len(phase.context.get_events(phase)) == 0  # No dropped row warning
