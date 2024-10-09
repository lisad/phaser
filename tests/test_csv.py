import numpy as np
import pytest
from phaser import Pipeline, Phase, DataErrorException
from phaser.io import read_csv, save_csv


@pytest.fixture
def temp_file(tmpdir):
    return tmpdir / 'temp_file.csv'


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

    warning = phase.context.get_events(phase=phase, row_num=1)[0]
    assert 'Extra value found' in warning['message']


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
def test_comment_lines(temp_file):
    write_text(temp_file, "#crew\n,id,name\n,1,James Kirk\n")
    assert read_csv(temp_file) == [{'id':1, 'name':'James Kirk'}]


def test_empty_lines_at_end(temp_file):
    write_text(temp_file, "id,name\n1,James Kirk\n\n\n")
    assert dict(read_csv(temp_file)[0]) == {'id':'1', 'name':'James Kirk'}


def test_empty_lines_elsewhere(temp_file):
    write_text(temp_file, "id,name\n\n1,James Kirk\n")
    assert dict(read_csv(temp_file)[0]) == {'id':'1', 'name':'James Kirk'}


@pytest.mark.skip("An empty line at the beginning of the file doesn't work. I think we can live with that")
def test_empty_line_at_beginning(temp_file):
    write_text(temp_file, "\nid,name\n1,James Kirk\n")
    assert dict(read_csv(temp_file)[0]) == {'id':'1', 'name':'James Kirk'}


def test_empty_line_only_commas(temp_file):
    write_text(temp_file, "id,name\n\n1,James Kirk\n,\n")
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}
    assert len(read_csv(temp_file)) == 1


def test_regular_quotes(temp_file):
    write_text(temp_file, '"id","name"\n1,James Kirk\n')
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}


def test_curious_quote_situation(temp_file):
    """ This is NOT how we think field names should ideally be determined. Note how the 'name' field
    name is not 'name' but actually ' "name"'.  Humans edit CSVs and add spaces so this happens IRL.
    It's OK though, we can strip spaces and quotes when we canonicalize column names in a regular phase"""
    write_text(temp_file, '"id", "name"\n1,James Kirk\n')
    assert dict(read_csv(temp_file)[0]) == {'id': '1', ' "name"': 'James Kirk'}


def test_do_not_save_nans(temp_file):
    save_csv(temp_file, [{"id": 1, "val": np.nan}, {"id": 2, 'val': 2}])
    assert "nan" not in read_text(temp_file).lower()


def test_pound_start_line(temp_file):
    write_text(temp_file,"Label,location,type\n#1,cabinet,yarn\n#2,garage,fiber")
    assert dict(read_csv(temp_file)[0]) == {'Label': "#1", 'location':'cabinet', 'type': 'yarn'}


def test_drop_empty_tab_delimiter(temp_file):
    write_text(temp_file, "\n\n\nid\tname\n1\tJames Kirk\n")
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}


def test_drop_empty_semicolon_delimiter(temp_file):
    write_text(temp_file, "\n\n\nid;name\n1;James Kirk\n")
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}


def test_drop_empty_large_file(temp_file):
    rows = ["id,name"] + [f"{i},name_{i}" for i in range(10000)]
    write_text(temp_file, "\n".join(rows)+"\n"*10000)
    data = read_csv(temp_file)
    assert len(data) == 10000
    assert dict(data[9999]) == {'id': '9999', 'name': 'name_9999'}


def test_drop_empty_non_utf8_encoding(temp_file):
    write_text(temp_file, "\n\nid,name\n1,José\n")
    temp_file.write_binary(temp_file.read_binary().replace(b'utf8', b'latin1'))
    data = read_csv(temp_file)
    assert dict(data[0]) == {'id': '1', 'name': 'José'}


def test_drop_empty_mixed_data_types(temp_file):
    write_text(temp_file, "\n\nid,age,is_student\n1,21,True\n2,22,False\n")
    data = read_csv(temp_file)
    assert dict(data[0]) == {'id': '1', 'age': '21', 'is_student': 'True'}
    assert dict(data[1]) == {'id': '2', 'age': '22', 'is_student': 'False'}


def test_string_with_comma(temp_file):
    # The comma within quotes should not make two fields load as 3; saving the CSV should preserve this
    write_text(temp_file, "Location name,id\n\"Southern New England Trunkline Trail, Grove Street\",20187\n")
    data = read_csv(temp_file)
    assert len(data[0]) == 2
    save_csv(temp_file, data)
    reloaded = read_csv(temp_file)
    assert len(reloaded[0]) == 2
