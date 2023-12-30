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


def test_check_unique_strips_spaces():
    fn = check_unique('id')
    with pytest.raises(AssertionError):
        fn(None, [{'id': " 1 "}, {'id': '1'}])


def test_check_unique_without_stripping():
    fn = check_unique('name', strip=False)
    fn(None, [{'name': '  Sam'}, {'name': 'Sam'}])


def test_check_unique_case_sensitive():
    fn = check_unique('dept')
    fn(None, [{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "eng"}])


def test_check_unique_case_insensitive():
    fn = check_unique('dept', ignore_case=True)
    with pytest.raises(AssertionError):
        fn(None, [{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "Eng"}])
