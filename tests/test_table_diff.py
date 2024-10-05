import pytest
from unittest.mock import Mock, MagicMock
from difflib import SequenceMatcher

from phaser.table_diff import IndexedTableDiffer, HtmlTableFormat

@pytest.fixture
def basic_table():
    return {
        1: {'planet': "Aaamazzara", 'homeworld': "Aaamazzarite"},
        2: {'planet': "Zetar", 'homeworld': "Zetarian"}
    }

def test_init(basic_table):
    differ = IndexedTableDiffer(basic_table, basic_table)
    assert differ.old_and_new_columns == [('planet', 'planet'), ('homeworld', 'homeworld')]
    assert differ.all_row_nums == [1, 2]

def test_added_column(basic_table):
    changed_table = basic_table.copy()
    changed_table[1]['destroyed'] = True
    differ = IndexedTableDiffer(basic_table, changed_table)
    assert differ.old_and_new_columns == [('planet', 'planet'), ('homeworld', 'homeworld'), ('destroyed', 'destroyed')]

def test_renamed_column(basic_table):
    changed_table = {
        1: {'LOCATION': "Aaamazzara", 'homeworld': "Aaamazzarite"},
    }
    differ = IndexedTableDiffer(basic_table, changed_table, column_renames={'planet': 'LOCATION'})
    assert differ.old_and_new_columns == [('planet', 'LOCATION'), ('homeworld', 'homeworld')]

def test_mistake_in_column_renames(basic_table):
    with pytest.raises(Exception) as exc:
        differ = IndexedTableDiffer(basic_table, basic_table, column_renames={'planet': 'LOCATION'})

def test_deleted_row(basic_table):
    changed_table = {1: basic_table[1]}
    differ = IndexedTableDiffer(basic_table, changed_table)
    differ.formatter = Mock()
    differ.iterate_rows()
    assert differ.counters['removed'] == 1

def test_added_row(basic_table):
    changed_table = basic_table.copy()
    changed_table[3] = {'planet': "Legara IV", "homeworld": "Legarians"}
    differ = IndexedTableDiffer(basic_table, changed_table)
    differ.formatter = Mock()
    differ.iterate_rows()
    assert differ.counters['added'] == 1

def test_diff_row():
    simple_table1 = {1: {'planet': 'Gemaris V'}}
    simple_table2 = {1: {'planet': 'Gemaris'}}
    differ = IndexedTableDiffer(simple_table1, simple_table2)
    differ.formatter = MagicMock()
    # Ignoring the actual contents of the table, going straight to diffing arbitrary rows:
    differ.iterate_rows()
    differ.formatter.show_changes.assert_called()
    differ.formatter.new_changed_row.assert_called()

def test_unchanged_row(basic_table):
    differ = IndexedTableDiffer(basic_table, basic_table)
    differ.formatter = MagicMock()
    differ.iterate_rows()
    differ.formatter.new_same_row.assert_called()

def test_infield_changes():
    diff_matcher = SequenceMatcher(None, "Khitomer", "Qi'tomer")
    formatter = HtmlTableFormat(old_and_new_columns=[('planet', 'planet')])
    display = formatter.show_changes(diff_matcher.get_opcodes(), "Khitomer", "Qi'tomer")
    assert 'deltext' in display
    assert 'newtext' in display
    assert "Khitomer" not in display  # it should now be broken up with spans, not a whole word

def test_format_deleted_row():
    diff_matcher = SequenceMatcher(None, "Gemaris V", "Gemaris")
    formatter = HtmlTableFormat(old_and_new_columns=[('planet', 'planet')])
    display = formatter.show_changes(diff_matcher.get_opcodes(), "Gemaris V", "Gemaris")
    assert 'Gemaris' in display
    assert 'deltext' in display