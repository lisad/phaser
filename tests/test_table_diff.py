import pytest
from unittest.mock import Mock, MagicMock
from difflib import SequenceMatcher

from phaser.table_diff import Differ, HtmlTableOutput

@pytest.fixture
def basic_table():
    return {
        1: {'planet': "Aaamazzara", 'homeworld': "Aaamazzarite"},
        2: {'planet': "Zetar", 'homeworld': "Zetarian"}
    }

def test_init(basic_table):
    differ = Differ(basic_table, basic_table)
    assert differ.all_field_names == ['planet', 'homeworld']
    assert differ.all_row_nums == [1, 2]

def test_added_column(basic_table):
    changed_table = basic_table.copy()
    changed_table[1]['destroyed'] = True
    differ = Differ(basic_table, changed_table)
    assert differ.all_field_names == ['planet', 'homeworld', 'destroyed']

def test_deleted_row(basic_table):
    changed_table = {1: basic_table[1]}
    differ = Differ(basic_table, changed_table)
    differ.outputter = Mock()
    differ.iterate_rows()
    assert differ.counters['removed'] == 1

def test_added_row(basic_table):
    changed_table = basic_table.copy()
    changed_table[3] = {'planet': "Legara IV", "homeworld": "Legarians"}
    differ = Differ(basic_table, changed_table)
    differ.outputter = Mock()
    differ.iterate_rows()
    assert differ.counters['added'] == 1

def test_diff_row():
    # Could do better here - not sure whether to make a custom Outputter as a test harness, or a smarter Mock
    simple_table = {1: {'planet': 'foo'}}
    differ = Differ(simple_table, simple_table)
    differ.outputter = MagicMock()
    # Ignoring the actual contents of the table, going straight to diffing arbitrary rows:
    differ.diff_row(1, {'planet': "Gemaris V"}, {'planet': "Gemaris"})
    differ.outputter.show_changes.assert_called()


def test_infield_changes():
    diff_matcher = SequenceMatcher(None, "Khitomer", "Qi'tomer")
    outputter = HtmlTableOutput(all_field_names=['planet'])
    display = outputter.show_changes(diff_matcher.get_opcodes(), "Khitomer", "Qi'tomer")
    print(display)
    assert display == """<span style="color: red">Kh</span><span style="color: green">Q</span>i<span style="color: green">'</span>tomer"""
