import pytest
from phaser import Phase, row_step, batch_step, WarningException, DropRowException
from phaser.phase import PhaseRecords

@row_step
def futz_with_row_num(row, **kwargs):
    row.row_num = row.row_num + 100
    return row

@row_step
def error_on_row_three(row, **kwargs):
    if row.row_num == 3:
        raise WarningException("Row three warning!")
    return row

@row_step
def drop_row_four(row, **kwargs):
    if row.row_num == 4:
        raise DropRowException("Row four dropped!")
    return row

@row_step
def return_native_dict(row, **kwargs):
    return { k: v for k, v in row.items() }

@pytest.mark.parametrize("steps, expected_row_nums",
    [
        ([futz_with_row_num], [0, 1, 2, 3, 4, 5]),
        ([error_on_row_three], [0, 1, 2, 3, 4, 5]),
        ([drop_row_four], [0, 1, 2, 3, 5]),
        ([error_on_row_three, drop_row_four], [0, 1, 2, 3, 5]),
        ([return_native_dict], [0, 1, 2, 3, 4, 5]),
    ]
)
def test_row_step_preserves_row_nums(steps, expected_row_nums):
    data = [
        { 'row': 0, 'num': 'zero'},
        { 'row': 1, 'num': 'one'},
        { 'row': 2, 'num': 'two'},
        { 'row': 3, 'num': 'three'},
        { 'row': 4, 'num': 'four'},
        { 'row': 5, 'num': 'five'},
    ]
    phase = Phase(steps=steps)
    phase.load_data(data)
    phase.run_steps()
    row_data = phase.row_data
    row_nums = [ r.row_num for r in row_data ]
    assert row_nums == expected_row_nums

@batch_step
def remove_odd_rows(batch, **kwargs):
    return [ row for index, row in enumerate(batch) if index % 2 == 0 ]

@batch_step
def sum_a_column(batch, **kwargs):
    new_batch = [batch[0]]
    for row in batch[1:]:
        last_row = new_batch[-1]
        if row['id'] == last_row['id']:
            last_row['n'] = last_row['n'] + row['n']
        else:
            new_batch.append(row)
    return new_batch

# This step resets the row numbers because it creates a new list(dict) rather
# than preserving the PhaseRecord objects that are in the batch already.
@batch_step
def accidentally_resets_row_nums(batch, **kwargs):
    new_batch = [
        { k: v for k, v in row.items() }
        for row in batch
    ]
    return new_batch[1:4]

@pytest.mark.parametrize("steps, expected_row_nums",
    [
        ([remove_odd_rows], [0, 2, 4]),
        ([sum_a_column], [0, 3, 4]),
        ([accidentally_resets_row_nums], [0, 1, 2]),
    ]
)
def test_batch_step_preserves_row_num(steps, expected_row_nums):
    data = [
        { 'row': 0, 'id': 10, 'num': 'zero', 'n': 10 },
        { 'row': 1, 'id': 10, 'num': 'one', 'n': 15 },
        { 'row': 2, 'id': 10, 'num': 'two', 'n': 20 },
        { 'row': 3, 'id': 11, 'num': 'three', 'n': 10 },
        { 'row': 4, 'id': 12, 'num': 'four', 'n': 10 },
        { 'row': 5, 'id': 12, 'num': 'five', 'n': 15 },
    ]
    phase = Phase(steps=steps)
    phase.load_data(data)
    phase.run_steps()
    row_data = phase.row_data
    row_nums = [ r.row_num for r in row_data ]
    assert row_nums == expected_row_nums
