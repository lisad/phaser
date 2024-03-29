import numpy as np
import pytest
from phaser import Pipeline, Phase, DataErrorException
from phaser.io import read_csv, save_csv


def write_text(path, text):
    return path.write_text(text, encoding='utf8')


def read_text(path):
    return path.read_text(encoding='utf8')


def test_duplicate_column_names(tmpdir):
    write_text(tmpdir / 'dupe-column-name.csv', "id,name,name\n1,Percy,Jackson\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'dupe-column-name.csv')
    with pytest.raises(DataErrorException):
        pipeline.load(tmpdir / 'dupe-column-name.csv')


def test_extra_field_in_csv(tmpdir):
    write_text(tmpdir / 'extra-field.csv', "id,name,age\n1,James Kirk,42,\n")
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
    write_text(tmpdir / 'insufficient-field.csv', "id,name,age\n1,James Kirk\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'insufficient-field.csv')
    data = pipeline.load(tmpdir / 'insufficient-field.csv')
    phase = Phase()
    phase.load_data(data)
    phase.do_column_stuff()

    assert len(phase.context.warnings) == 1


@pytest.mark.skip("Commented lines are really hard to skip with clevercsv but otherwise it's a good library...")
def test_comment_lines(tmpdir):
    write_text(tmpdir / 'commented_line', "#crew\n,id,name\n,1,James Kirk\n")
    assert read_csv(tmpdir / 'commented_line') == [{'id':1, 'name':'James Kirk'}]


def test_empty_lines_at_end(tmpdir):
    write_text(tmpdir / 'commented_line', "id,name\n1,James Kirk\n\n\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


def test_empty_lines_elsewhere(tmpdir):
    write_text(tmpdir / 'commented_line', "id,name\n\n1,James Kirk\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


@pytest.mark.skip("An empty line at the beginning of the file doesn't work. I think we can live with that")
def test_empty_line_at_beginning(tmpdir):
    write_text(tmpdir / 'commented_line', "\nid,name\n1,James Kirk\n")
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id':'1', 'name':'James Kirk'}


def test_regular_quotes(tmpdir):
    write_text(tmpdir / 'commented_line', '"id","name"\n1,James Kirk\n')
    assert dict(read_csv(tmpdir / 'commented_line')[0])== {'id': '1', 'name': 'James Kirk'}


def test_curious_quote_situation(tmpdir):
    """ This is NOT how we think field names should ideally be determined. Note how the 'name' field
    name is not 'name' but actually ' "name"'.  Humans edit CSVs and add spaces so this happens IRL.
    It's OK though, we can strip spaces and quotes when we canonicalize column names in a regular phase"""
    write_text(tmpdir / 'commented_line', '"id", "name"\n1,James Kirk\n')
    assert dict(read_csv(tmpdir / 'commented_line')[0]) == {'id': '1', ' "name"': 'James Kirk'}


def test_do_not_save_nans(tmpdir):
    data = [{"id": 1, "val": np.nan}, {"id": 2, 'val': 2}]
    save_csv(tmpdir / 'save_nans.csv', data)
    result = read_text(tmpdir / 'save_nans.csv')
    assert "nan" not in result.lower()