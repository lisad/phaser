from datetime import datetime, date

import pytest
from dateutil.tz import gettz

from phaser import Phase, Column, IntColumn, DateColumn, DateTimeColumn, PipelineErrorException, DropRowException


def test_null_forbidden_but_null_default():
    with pytest.raises(Exception):
        Column(name='bogus', null=False, default='homer')


def test_required_values():
    mycol = Column('crew', allowed_values=["Kirk", "Riker", "Troi", "Crusher"])
    mycol.check_and_cast_value({"crew": "Kirk", 'position': "Captain"})
    with pytest.raises(PipelineErrorException):
        mycol.check_and_cast_value({"crew": "Gilligan", 'position': "mate"})


def test_null_forbidden():
    col = Column('employeeid', null=False)
    with pytest.raises(PipelineErrorException):
        col.check_and_cast_value({'employeeid': None})


def test_default_value():
    col = Column('location', default='HQ')
    input = [{'location': 'Atlanta'}, {'location': None}]
    data = [col.check_and_cast_value(row) for row in input]
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
    phase.load_data([{'dept': 'Eng', 'dob': '20000101'}, {'division': 'Accounting', 'birthdate': '19820101'}])
    phase.rename_columns()
    assert all(list(row.keys()) == ['department', 'birth_date'] for row in phase.row_data)
    assert phase.headers == ['department', 'birth_date']


def test_canonicalize_names():
    col1 = Column("Country of Origin")
    phase = Phase(columns=[col1])
    phase.load_data([{'country of origin': 'UK'}, {'country_of_origin': 'US'}])
    phase.rename_columns()
    assert all(list(row.keys()) == ['Country of Origin'] for row in phase.row_data)
    assert phase.headers == ['Country of Origin']


def test_forbidden_column_name_characters():
    with pytest.raises(AssertionError):
        Column('1\n2\n3')
    with pytest.raises(AssertionError):
        Column('a\tb\tc')


def test_int_column_casts():
    col = IntColumn(name="Age", min_value=0)
    phase = Phase(columns=[col])
    phase.load_data([{'age': "3"}, {'age': "4 "}, {'age': "5.0"}])
    phase.do_column_stuff()
    assert [row['Age'] for row in phase.row_data] == [3, 4, 5]


def test_int_column_null_value():
    col = IntColumn(name="Age")
    assert col.cast(None) is None


def test_int_column_minmax():
    col = IntColumn(name="Age", min_value=0, max_value=130)
    with pytest.raises(PipelineErrorException):
        col.check_value(-1)
    with pytest.raises(PipelineErrorException):
        col.check_value(2000)


def test_datetime_column_casts():
    col = DateTimeColumn(name="start")
    with pytest.raises(Exception):
        col.cast('Data')
    assert col.cast("2223/01/01 14:28") == datetime(2223,1,1, 14, 28)


def test_datetime_column_custom_format():
    col=DateTimeColumn(name="stardate", date_format_code="%Y%m%d")
    assert col.cast("22230101") == datetime(2223,1,1)


def test_datetime_column_applies_tz():
    col = DateTimeColumn(name="start", default_tz=gettz("America/Los Angeles"))
    value = col.cast("22230101")
    assert value.tzname() == "PST"


def test_date_column_casts_to_date():
    col = DateColumn(name="start")
    assert col.cast("2223/01/01") == date(2223,1,1)


def test_date_column_range():
    col = DateColumn(name="start", min_value=date(2019, 12, 1), max_value=date.today())
    col.check_and_cast_value({'start': "2024-01-14"})
    with pytest.raises(PipelineErrorException):
        col.check_and_cast_value({'start': "2012-01-01"})

def test_column_error_selection():
    col = Column(name='room', allowed_values=['stateroom', 'cabin'], on_error='drop_row')
    row = {'room': 'single'}
    with pytest.raises(DropRowException):
        col.check_and_cast_value(row)

# LMDTODO Add a test that if required=False, and the column is triggered to cast values, and its not there, it's OK

# Add a test that rows are really dropped

# Add a test that a custom column "MyColumn" can override the 'cast' method and apply a custom error handling by
# throwing an exception

# Test the output of warnings