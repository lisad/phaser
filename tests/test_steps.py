from pathlib import Path
import pytest

from phaser import check_unique, Phase
from fixtures import test_data_phase_class

current_path = Path(__file__).parent


def test_builtin_step():
    phase = Phase(steps=[check_unique('crew id')])
    phase.load(current_path / 'fixture_files' / 'crew.csv')
    phase.run_steps()


def test_check_unique_fails(test_data_phase_class):
    phase = test_data_phase_class()
    phase.row_data = [
        {'id': '1'},
        {'id': '1'}
    ]
    with pytest.raises(AssertionError):
        phase.run_steps()


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
