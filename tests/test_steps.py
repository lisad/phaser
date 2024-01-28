from pathlib import Path
import pytest

from phaser import check_unique, Phase, row_step, PipelineErrorException, Pipeline, sort_by, IntColumn
from fixtures import test_data_phase_class

current_path = Path(__file__).parent


# Tests of the operation of steps

def test_context_available_to_step():
    @row_step
    def replace_value_fm_context(row, context):
        row['secret'] = context.get('secret')
        return row

    transformer = Phase(steps=[replace_value_fm_context])
    transformer.context.add_variable('secret', "I'm always angry")
    transformer.load_data([{'id': 1, 'secret': 'unknown'}])
    transformer.run_steps()
    assert transformer.row_data[0]['secret'] == "I'm always angry"


# Tests of the check_unique step


def test_builtin_step():
    phase = Phase(steps=[check_unique('crew id')])
    phase.load(current_path / 'fixture_files' / 'crew.csv')
    phase.run_steps()


def test_check_unique_fails(test_data_phase_class):
    phase = test_data_phase_class(error_policy=Pipeline.ON_ERROR_STOP_NOW)
    phase.load_data([{'id': '1'}, {'id': '1'}])
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


# Testing builtin sort step

def test_with_col_name():
    phase = Phase(steps=[sort_by("id")])
    phase.load_data([{'id': 1}, {'id': 3}, {'id': 2}, {'id': 0}])
    phase.run_steps()
    assert all([phase.row_data[i]['id'] == i for i in range(4)])

def test_with_col_obj():
    id_col = IntColumn(name='id')
    phase = Phase(columns=[id_col], steps=[sort_by(id_col)])
    phase.load_data([{'id': 1}, {'id': 3}, {'id': 2}, {'id': 0}])
    phase.run_steps()
    assert all([phase.row_data[i]['id'] == i for i in range(4)])
