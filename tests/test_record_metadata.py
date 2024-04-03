import pytest
from phaser import Phase, row_step, batch_step, WarningException, DropRowException

@row_step
def futz_with_row_num(row, **kwargs):
    row.row_num = row.row_num + 100
    return row

@row_step
def error_on_row_four(row, **kwargs):
    if row.row_num == 4:
        raise WarningException("Row four warning!")
    return row

@row_step
def drop_row_five(row, **kwargs):
    if row.row_num == 5:
        raise DropRowException("Row five dropped!")
    return row

@row_step
def return_native_dict(row, **kwargs):
    return { k: v for k, v in row.items() }

@pytest.mark.parametrize("steps, expected_row_nums",
    [
        #([futz_with_row_num], [1, 2, 3, 4, 5, 6]),  JEFF this now causes error - shouldn't it?
        ([error_on_row_four], [1, 2, 3, 4, 5, 6]),
        ([drop_row_five], [1, 2, 3, 4, 6]),
        ([error_on_row_four, drop_row_five], [1, 2, 3, 4, 6]),
        ([return_native_dict], [1, 2, 3, 4, 5, 6]),
    ]
)
def test_row_step_preserves_row_nums(steps, expected_row_nums):
    data = [
        { 'row': 1, 'num': 'zero'},
        { 'row': 2, 'num': 'one'},
        { 'row': 3, 'num': 'two'},
        { 'row': 4, 'num': 'three'},
        { 'row': 5, 'num': 'four'},
        { 'row': 6, 'num': 'five'},
    ]
    phase = Phase(steps=steps)
    phase.load_data(data)
    phase.run_steps()
    row_data = phase.row_data
    row_nums = [ r.row_num for r in row_data ]
    assert row_nums == expected_row_nums

@batch_step
def remove_even_rows(batch, **kwargs):
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
        ([remove_even_rows], [1, 3, 5]),
        ([sum_a_column], [1, 4, 5]),
        ([accidentally_resets_row_nums], [7, 8, 9]),  # If the row numbers are gone, these look like *new* rows
    ]
)
def test_batch_step_preserves_row_num(steps, expected_row_nums):
    data = [
        { 'row': 1, 'id': 10, 'num': 'zero', 'n': 10 },
        { 'row': 2, 'id': 10, 'num': 'one', 'n': 15 },
        { 'row': 3, 'id': 10, 'num': 'two', 'n': 20 },
        { 'row': 4, 'id': 11, 'num': 'three', 'n': 10 },
        { 'row': 5, 'id': 12, 'num': 'four', 'n': 10 },
        { 'row': 6, 'id': 12, 'num': 'five', 'n': 15 },
    ]
    phase = Phase(steps=steps)
    phase.load_data(data)
    phase.run_steps()
    row_data = phase.row_data
    row_nums = [ r.row_num for r in row_data ]
    assert row_nums == expected_row_nums
