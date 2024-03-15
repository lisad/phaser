import pytest
from phaser import Pipeline, Phase
from phaser.util import read_csv

def test_duplicate_column_names(tmpdir):
    with open(tmpdir / 'dupe-column-name.csv', 'w') as f:
        f.write("id,name,name\n1,Percy,Jackson\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'dupe-column-name.csv')
    with pytest.raises(Exception):
        pipeline.load(tmpdir / 'dupe-column-name.csv')


def test_extra_field_in_csv(tmpdir):
    with open(tmpdir / 'extra-field.csv', 'w') as f:
        f.write("id,name,age\n1,James Kirk,42,\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'extra-field.csv')
    data = pipeline.load(tmpdir / 'extra-field.csv')
    phase = Phase()
    phase.load_data(data)
    phase.do_column_stuff()

    assert len(phase.context.warnings) == 1
    print(phase.context.warnings)
    assert 'Extra value found' in phase.context.warnings[1][0]['message']


def test_comment_lines(tmpdir):
    # TODO assuming we write code to make this work - parametrize comments on different lines
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("#crew\n,id,name\n,1,James Kirk\n")
    assert read_csv(tmpdir / 'commented_line') == [{'id':1, 'name':'James Kirk'}]


def test_empty_lines(tmpdir):
    # TODO assuming we write code to make this work - parametrize empty lines in different places
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("\nid,name\n,1,James Kirk\n")
    assert read_csv(tmpdir / 'commented_line') == [{'id':1, 'name':'James Kirk'}]


def test_strip_spaces(tmpdir):
    # TODO assuming we write code to make this work - test stripping from values as well as headers
    with open(tmpdir / 'extra_spaces', 'w') as f:
        f.write("  id,name\n,1,James Kirk\n")
    assert read_csv(tmpdir / 'extra_spaces') == [{'id':1, 'name':'James Kirk'}]
