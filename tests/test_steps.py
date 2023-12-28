import pytest

from fixtures import transform_employees_phase
from phaser.steps import check_unique


def test_builtin_step(transform_employees_phase):
    transform_employees_phase.steps = [
        check_unique('employee id')
    ]
    transform_employees_phase.load()
    transform_employees_phase.run_steps()


def test_check_unique_fails(transform_employees_phase):
    transform_employees_phase.steps = [
        check_unique('employee id')
    ]
    transform_employees_phase.row_data = [
        {'employee id': '1'},
        {'employee id': '1'}
    ]
    with pytest.raises(AssertionError):
        transform_employees_phase.run_steps()
