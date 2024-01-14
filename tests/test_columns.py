import pytest

from phaser import Phase, Column, IntColumn


def test_null_forbidden_but_null_default():
    with pytest.raises(Exception):
        Column(name='bogus', null=False, default='homer')


def test_required_values():
    mycol = Column('crew', allowed_values=["Kirk", "Riker", "Troi", "Crusher"])
    mycol.check_and_cast(["crew"], [{"crew": "Kirk"}, {"crew": "Riker"}])
    with pytest.raises(Exception):
        mycol.check_value("Gilligan")


def test_null_forbidden():
    col = Column('employeeid', null=False)
    with pytest.raises(ValueError):
        col.check_and_cast(['employeeid'], [{'employeeid': None}])


def test_default_value():
    col = Column('location', default='HQ')
    input = [{'location': 'Atlanta'}, {'location': None}]
    data = col.check_and_cast(['location'], input)
    for row in data:
        if row[col.name] is None:
            assert col.fix_value(row[col.name]) == 'HQ'
        else:
            assert col.fix_value(row[col.name]) == row[col.name]


def test_fix_value_fn_instance_method():
    col = Column('location', fix_value_fn='lstrip')
    assert col.fix_value('  Toronto') == 'Toronto'


def test_abs_on_int_column():
    col = IntColumn('value', fix_value_fn='abs')
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


def test_rename():
    # Rename is done during the Phase importing data
    col1 = Column('department', rename=['dept', 'division'])
    col2 = Column('birth_date', rename=['dob', 'birthdate'])
    phase = Phase(columns=[col1, col2])
    # Normally rowdata imported from CSV will have only one variant per file but this should work too
    # in case people import from inconsistent JSON
    phase.row_data = [{'dept': 'Eng', 'dob': '20000101'}, {'division': 'Accounting', 'birthdate': '19820101'}]
    phase.rename_columns()
    assert all(list(row.keys()) == ['department', 'birth_date'] for row in phase.row_data)


def test_canonicalize_names():
    col1 = Column("Country of Origin")
    phase = Phase(columns=[col1])
    phase.row_data = [{'country of origin': 'UK'}, {'country_of_origin': 'US'}]
    phase.rename_columns()
    assert all(list(row.keys()) == ['Country of Origin'] for row in phase.row_data)


def test_forbidden_column_name_characters():
    with pytest.raises(AssertionError):
        Column('1\n2\n3')
    with pytest.raises(AssertionError):
        Column('a\tb\tc')

def test_int_column_casts():
    col = IntColumn(name="Age", min_value=0)
    phase = Phase(columns=[col])
    phase.row_data = [{'age': "3"}, {'age': "4 "}, {'age': "5.0"}]
    phase.do_column_stuff()
    assert [row['Age'] for row in phase.row_data] == [3,4,5]

def test_int_column_null_value():
    col = IntColumn(name="Age")
    assert col.cast(None) is None

def test_int_column_minmax():
    col = IntColumn(name="Age", min_value=0, max_value=130)
    with pytest.raises(ValueError):
        col.check_value(-1)
    with pytest.raises(ValueError):
        col.check_value(2000)