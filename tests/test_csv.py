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


def test_read_extra_field_in_csv(tmpdir):
    write_text(tmpdir / 'extra-field.csv', "id,name,age\n1,James Kirk,42,\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'extra-field.csv')
    data = pipeline.load(tmpdir / 'extra-field.csv')
    assert all([len(row.keys()) == 3 for row in data])


def test_read_not_enough_fields_in_csv(tmpdir):
    write_text(tmpdir / 'insufficient-field.csv', "id,name,age\n1,James Kirk\n")
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'insufficient-field.csv')
    with pytest.raises(Exception) as exc_info:
        pipeline.load(tmpdir / 'insufficient-field.csv')
        assert ("missing" in exc_info.value)


def test_read_comment_lines(temp_file):
    write_text(temp_file, "#crew\nid,name\n1,James Kirk\n")
    assert read_csv(temp_file) == [{'id': '1', 'name': 'James Kirk'}]


@pytest.mark.parametrize('file_contents',
                         [
                            "id,name\n1,James Kirk\n\n\n",  # Empty lines at the end
                            "id,name\n1,James Kirk\n   \n   \n   ",  # also with spaces
                            "id,name\n\n1,James Kirk\n",    # Empty line in middle
                            "\nid,name\n1,James Kirk\n",    # Empty line in beginning
                            "id,name\n\n1,James Kirk\n,\n"  # Line with only commas
                         ])
def test_read_files_with_empty_lines(temp_file, file_contents):
    write_text(temp_file, file_contents)
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}
    assert len(read_csv(temp_file)) == 1


def test_read_regular_quotes(temp_file):
    write_text(temp_file, '"id","name"\n1,James Kirk\n')
    assert dict(read_csv(temp_file)[0]) == {'id': '1', 'name': 'James Kirk'}


def test_read_curious_quote_situation(temp_file):
    """ This is NOT how we think field names should ideally be determined. Note how the 'name' field
    name is not 'name' but actually ' "name"'.  Humans edit CSVs and add spaces so this happens IRL.
    It's OK though, we can strip spaces and quotes when we canonicalize column names in a regular phase"""
    write_text(temp_file, '"id", "name"\n1,James Kirk\n')
    assert dict(read_csv(temp_file)[0]) == {'id': '1', ' "name"': 'James Kirk'}


def test_save_do_not_save_nans(temp_file):
    save_csv(temp_file, [{"id": 1, "val": np.nan}, {"id": 2, 'val': 2}])
    assert "nan" not in read_text(temp_file).lower()


def test_read_pound_start_line(temp_file):
    write_text(temp_file, "Label,location,type\n#1,cabinet,yarn\n#2,garage,fiber")
    assert dict(read_csv(temp_file)[0]) == {'Label': "#1", 'location': 'cabinet', 'type': 'yarn'}


def test_read_drop_empty_tab_delimiter(temp_file):
    write_text(temp_file, "\n\n\nid\tname\n1\tJames Kirk\n")
    assert dict(read_csv(temp_file, delimiter='\t')[0]) == {'id': '1', 'name': 'James Kirk'}


def test_read_drop_empty_semicolon_delimiter(temp_file):
    write_text(temp_file, "\n\n\nid;name\n1;James Kirk\n")
    assert dict(read_csv(temp_file, delimiter=';')[0]) == {'id': '1', 'name': 'James Kirk'}


def test_read_drop_empty_large_file(temp_file):
    rows = ["id,name"] + [f"{i},name_{i}" for i in range(10000)]
    write_text(temp_file, "\n".join(rows) + "\n" * 10000)
    data = read_csv(temp_file)
    assert len(data) == 10000
    assert dict(data[9999]) == {'id': '9999', 'name': 'name_9999'}


def test_read_drop_empty_non_utf8_encoding(temp_file):
    write_text(temp_file, "\n\nid,name\n1,José\n")
    temp_file.write_binary(temp_file.read_binary().replace(b'utf8', b'latin1'))
    data = read_csv(temp_file)
    assert dict(data[0]) == {'id': '1', 'name': 'José'}


def test_read_drop_empty_mixed_data_types(temp_file):
    write_text(temp_file, "\n\nid,age,is_student\n1,21,True\n2,22,False\n")
    data = read_csv(temp_file)
    assert dict(data[0]) == {'id': '1', 'age': '21', 'is_student': 'True'}
    assert dict(data[1]) == {'id': '2', 'age': '22', 'is_student': 'False'}


def test_read_string_with_comma(temp_file):
    # The comma within quotes should not make two fields load as 3; saving the CSV should preserve this
    write_text(temp_file, "Location name,id\n\"Southern New England Trunkline Trail, Grove Street\",20187\n")
    data = read_csv(temp_file)
    assert len(data[0]) == 2
    save_csv(temp_file, data)
    reloaded = read_csv(temp_file)
    assert len(reloaded[0]) == 2


@pytest.mark.parametrize("text, expected",
                         [
                             ("id\n3\n", [{'id': '3'}]),
                             # This one resulted in [{'id':'abc'},{None:['def']}] when using clever_csv
                             ("id\nabc def\n", [{'id': 'abc def'}]),
                             ("on_call\nJulian\n", [{'on_call': 'Julian'}]),
                             # This one resulted in a NoDetectionResult exception when using clever_csv
                             ("on_call\nJulian Bashir\n", [{'on_call': 'Julian Bashir'}]),
                         ]
                         )
def test_read_single_row_col_csv(tmpdir, text, expected):
    path = tmpdir / "data.csv"
    write_text(path, text)
    data = read_csv(path)
    assert data == expected


def test_save_extra_fields(temp_file):
    # Data that has extra fields added to just some rows, or loaded from JSON where
    # some JSON records may have extra fields
    save_csv(temp_file, [{"id": 1, "val": 1}, {"id": 2, 'val': 2, 'extra': 'yes'}])
    new_data = read_csv(temp_file)
    assert new_data[0] == {"id": '1', "val": '1', 'extra': ''}


def test_array_values_quoted(temp_file):
    # Data with array values gets quoted for safety.  We don't automatically assume that data that's imported that
    # looks like an array ought to be forced to an array - we would need an ArrayColumn type for that.
    data = [{"id": "1", "log_entries": ['338822', '45380']}]
    save_csv(temp_file, data)
    new_data = read_csv(temp_file)
    assert new_data[0]['log_entries'] == "['338822', '45380']"
