from pathlib import Path
import pytest

from phaser import check_unique, Phase, row_step, PipelineErrorException, Pipeline
from fixtures import test_data_phase_class

current_path = Path(__file__).parent


def test_builtin_step():
    phase = Phase(steps=[check_unique('crew id')])
    phase.load(current_path / 'fixture_files' / 'crew.csv')
    phase.run_steps()


def test_check_unique_fails(test_data_phase_class):
    phase = test_data_phase_class(error_policy=Pipeline.ON_ERROR_STOP_NOW)
    phase.row_data = [
        {'id': '1'},
        {'id': '1'}
    ]
    with pytest.raises(PipelineErrorException):
        phase.run_steps()


def test_check_unique_strips_spaces():
    fn = check_unique('id')
    with pytest.raises(PipelineErrorException):
        fn([{'id': " 1 "}, {'id': '1'}])


def test_check_unique_without_stripping():
    fn = check_unique('name', strip=False)
    fn([{'name': '  Sam'}, {'name': 'Sam'}])


def test_check_unique_case_sensitive():
    fn = check_unique('dept')
    fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "eng"}])


def test_check_unique_case_insensitive():
    fn = check_unique('dept', ignore_case=True)
    with pytest.raises(PipelineErrorException):
        fn([{'dept': "ENG"}, {'dept': 'Sales'}, {'dept': "Eng"}])


def test_context_available_to_step():
    @row_step
    def replace_value_fm_context(row, context):
        row['secret'] = context.get('secret')
        return row

    transformer = Phase(steps=[replace_value_fm_context])
    transformer.context.add_variable('secret', "I'm always angry")
    transformer.row_data = [{'id': 1, 'secret': 'unknown'}]
    transformer.run_steps()
    assert transformer.row_data[0]['secret'] == "I'm always angry"
