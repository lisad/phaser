from pathlib import Path
import pytest

from phaser import check_unique, Phase, row_step
from fixtures import test_data_phase_class

current_path = Path(__file__).parent


def test_builtin_step():
    phase = Phase(steps=[check_unique('employee id')])
    phase.load(current_path / 'fixture_files' / 'employees.csv')
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
        fn([{'id': " 1 "}, {'id': '1'}])


def test_check_unique_without_stripping():
    fn = check_unique('name', strip=False)
    fn([{'name': '  Sam'}, {'name': 'Sam'}])


def test_check_unique_case_sensitive():
    fn = check_unique('dept')
    fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "eng"}])


def test_check_unique_case_insensitive():
    fn = check_unique('dept', ignore_case=True)
    with pytest.raises(AssertionError):
        fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "Eng"}])


def test_context_available_to_step():
    @row_step
    def replace_value_fm_context(row, context):
        row['secret'] = context['secret']
        return row

    transformer = Phase(steps=[replace_value_fm_context], context={'secret': "I'm always angry"})
    transformer.row_data = [ {'id': 1, 'secret': 'unknown'}]
    transformer.run_steps()
    assert transformer.row_data[0]['secret'] == "I'm always angry"
