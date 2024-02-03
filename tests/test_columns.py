from datetime import datetime, date

import numpy as np
import pytest
from dateutil.tz import gettz

from phaser import Phase, Column, IntColumn, DateColumn, DateTimeColumn, PipelineErrorException, DropRowException


# Constructor tests
def test_null_forbidden_but_null_default():
    with pytest.raises(Exception):
        Column(name='bogus', null=False, default='homer')


def test_invalid_on_error():
    col = Column(name="anything", on_error="Bogus")
    assert col.use_exception == PipelineErrorException


# Simple feature tests

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


# Tests of fix_value_fn


def test_fix_value_fn_instance_method():
    col = Column('location', fix_value_fn='lstrip')
    assert col.fix_value('  Toronto') == 'Toronto'


def test_fix_value_in_larger_context():
    col = Column('log_message', fix_value_fn='lstrip')
    phase = Phase(columns=col)
    phase.load_data([{'log_message': '  Stardate 20240127.12569: Nothing happened today.  '}])
    phase.do_column_stuff()
    assert phase.row_data[0]['log_message'] == 'Stardate 20240127.12569: Nothing happened today.  '


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


# Test naming fetaures


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


def test_rename_passed_string():
    col = Column(name='Crew ID', rename="crewNumber" )
    phase = Phase(columns=[col])
    phase.load_data([{'crewNumber': '1'}, {'crewNumber': '2'}])
    phase.rename_columns()
    assert phase.headers == [col.name]


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


# Testing IntColumn

def test_int_column_casts():
    col = IntColumn(name="Age", min_value=0)
    phase = Phase(columns=[col])
    phase.load_data([{'age': "3"}, {'age': "4 "}, {'age': "5.0"}])
    phase.do_column_stuff()
    assert [row['Age'] for row in phase.row_data] == [3, 4, 5]


def test_int_column_null_value():
    col = IntColumn(name="Age")
    assert col.cast(None) is None


def test_cast_nan_when_null_ok():
    col = IntColumn(name="Skill level")
    assert col.cast(np.nan) is None


def test_cast_when_not_present():
    col = IntColumn(name="Shoe size", required=False)
    phase = Phase(columns=[col])
    phase.load_data([{'TShirt size': "medium"}, {'Hat size': 21}])
    phase.do_column_stuff()
    assert all([row["Shoe size"] is None for row in phase.row_data])


def test_int_column_minmax():
    col = IntColumn(name="Age", min_value=0, max_value=130)
    with pytest.raises(PipelineErrorException):
        col.check_value(-1)
    with pytest.raises(PipelineErrorException):
        col.check_value(2000)


# Testing Datetime and date column

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


# Tests of column error handling


def test_column_error_selection():
    col = Column(name='room', allowed_values=['stateroom', 'cabin'], on_error='drop_row')
    row = {'room': 'single'}
    with pytest.raises(DropRowException):
        col.check_and_cast_value(row)


def test_column_can_drop_row():
    def drop_if_no_shoe_size(value):
        if value is None:
            raise DropRowException("We can't order those cool boots for alien crew members with no feet")
        return value

    col = IntColumn(name='Shoe size', fix_value_fn=drop_if_no_shoe_size)
    phase = Phase(columns=[col])
    phase.load_data([{'Shoe size': '42'}, {'Shoe size': None}])
    phase.do_column_stuff()
    assert len(phase.context.errors) == 0
    assert len(phase.row_data) == 1
    assert phase.row_data[0]['Shoe size'] == 42

