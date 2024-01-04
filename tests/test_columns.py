import pytest
from phaser import Column, Phase

def test_null_forbidden_but_null_default():
    with pytest.raises(Exception):
        Column(name='bogus', null=False, default='homer')

def test_required_values():
    mycol = Column('passenger', allowed_values=["Gilligan", "Skipper", "Professor"])
    mycol.check(["passenger"], [{"passenger": "Gilligan"}, {"passenger": "Skipper"}])
    with pytest.raises(Exception):
        mycol.check(["passenger"], [{"passenger": "Shakespeare"}])

def test_null_forbidden():
    col = Column('employeeid', null=False)
    with pytest.raises(ValueError):
        col.check(['employeeid'], [{'employeeid': None}])

def test_default_value():
    col = Column('location', default='HQ')
    data = [{'location': 'Atlanta'}, {'location': None}]
    col.check(['location'], data)
    for row in data:
        if row[col.name] is None:
            assert col.fix_value(row[col.name]) == 'HQ'
        else:
            assert col.fix_value(row[col.name]) == row[col.name]

def test_fix_value_fn_instance_method():
    col = Column('location', fix_value_fn='lstrip')
    assert col.fix_value('  Toronto') == 'Toronto'

def test_abs_on_int_column():
    #LMDTODO rewrite to actually use int column when that is built, and include transform to int
    col = Column('value', fix_value_fn='abs')
    assert col.fix_value(-1) == 1

def test_fix_value_fn_value_as_param():
    col = Column('keycode', fix_value_fn='bytearray')
    assert col.fix_value([1, 2, 200]) == bytearray(b'\x01\x02\xc8')

def test_callable():
    def my_func(string):
        return string.strip().capitalize()

    col = Column('team', fix_value_fn=my_func)
    assert col.fix_value("  RAPTORS  ") == "Raptors"

def test_multiple_functions():
    col = Column('status', fix_value_fn=['lstrip', 'capitalize'])
    assert col.fix_value("  ACTIVE  ") == "Active  "

def test_rename(tmpdir):
    # Rename is done during the Phase importing data
    col1 = Column('department', rename=['dept', 'division'])
    col2 = Column('birth_date', rename=['dob', 'birthdate'])
    phase = Phase(source='xyz.csv', working_dir=tmpdir, columns=[col1, col2])
    # Normally rowdata imported from CSV will have only one variant per file but this should work too
    # in case people import from inconsistent JSON
    phase.row_data = [{'dept': 'Eng', 'dob': '20000101'}, {'division': 'Accounting', 'birthdate': '19820101'}]
    phase.rename_columns()
    assert all(list(row.keys()) == ['department', 'birth_date'] for row in phase.row_data)

def test_canonicalize_names(tmpdir):
    col1 = Column("Country of Origin")
    phase = Phase(source='xyz.csv', working_dir=tmpdir, columns=[col1])
    phase.row_data = [{'country of origin': 'UK'}, {'country_of_origin': 'US'}]
    phase.rename_columns()
    assert all(list(row.keys()) == ['Country of Origin'] for row in phase.row_data)

def test_forbidden_column_name_characters():
    with pytest.raises(AssertionError):
        Column('1\n2\n3')
    with pytest.raises(AssertionError):
        Column('a\tb\tc')