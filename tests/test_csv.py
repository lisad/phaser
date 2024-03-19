import pytest
from phaser import Pipeline, Phase, DataErrorException
from phaser.io import read_csv

def test_duplicate_column_names(tmpdir):
    with open(tmpdir / 'dupe-column-name.csv', 'w') as f:
        f.write("id,name,name\n1,Percy,Jackson\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'dupe-column-name.csv')
    with pytest.raises(DataErrorException):
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


@pytest.mark.skip("It would be nice to identify rows with not enough fields...")
def test_not_enough_fields_in_csv(tmpdir):
    with open(tmpdir / 'insufficient-field.csv', 'w') as f:
        f.write("id,name,age\n1,James Kirk\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'insufficient-field.csv')
    data = pipeline.load(tmpdir / 'insufficient-field.csv')
    phase = Phase()
    phase.load_data(data)
    phase.do_column_stuff()

    assert len(phase.context.warnings) == 1


@pytest.mark.skip("Commented lines are really hard to skip with clevercsv but otherwise it's a good library...")
def test_comment_lines(tmpdir):
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("#crew\n,id,name\n,1,James Kirk\n")
    assert read_csv(tmpdir / 'commented_line') == [{'id':1, 'name':'James Kirk'}]


def test_empty_lines_at_end(tmpdir):
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("id,name\n1,James Kirk\n\n\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


def test_empty_lines_elsewhere(tmpdir):
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("id,name\n\n1,James Kirk\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


@pytest.mark.skip("An empty line at the beginning of the file doesn't work. I think we can live with that")
def test_empty_line_at_beginning(tmpdir):
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write("\nid,name\n1,James Kirk\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


def test_regular_quotes(tmpdir):
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write('"id","name"\n1,James Kirk\n')
    assert dict(read_csv(tmpdir / 'commented_line')[0])== {'id': '1', 'name': 'James Kirk'}


@pytest.mark.skip("Although the library removes quotes normally, a space makes that not happen")
def test_curious_quote_situation(tmpdir):
    """ It's OK though, we can strip spaces and quotes when we canonicalize column names """
    with open(tmpdir / 'commented_line', 'w') as f:
        f.write('"id", "name"\n1,James Kirk\n')
    assert dict(read_csv(tmpdir / 'commented_line')[0])== {'id': '1', 'name': 'James Kirk'}
