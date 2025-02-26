from datetime import datetime, date

import numpy as np
import pytest
from dateutil.tz import gettz

from phaser import (Phase, Column, IntColumn, FloatColumn, DateColumn, DateTimeColumn, BooleanColumn,
                    DataErrorException, DropRowException, ON_ERROR_DROP_ROW, PhaserError, row_step)


# Constructor tests
def test_null_forbidden_but_also_default():
    # Null=False should mean that the column can't have a null value or that creates an error.
    # This is incompatible with providing a default value that defines the value if the value is None.
    with pytest.raises(PhaserError):
        Column(name='bogus', null=False, default='homer')


def test_invalid_on_error():
    with pytest.raises(PhaserError) as excinfo:
        col = Column(name='anything', on_error='BOGUS')
    assert "Supported on_error values" in excinfo.value.message


@pytest.mark.parametrize('column_type', [IntColumn, FloatColumn, DateColumn, DateTimeColumn, BooleanColumn])
def test_non_string_column_types_dont_have_empty_param(column_type):
    with pytest.raises(TypeError):
        column_type(name='inconsistent', null=False, blank=True)


# Simple feature tests

def test_required_values():
    mycol = Column('crew', allowed_values=["Kirk", "Riker", "Troi", "Crusher"])
    mycol.check_and_cast_value({"crew": "Kirk", 'position': "Captain"})
    with pytest.raises(DataErrorException):
        mycol.check_and_cast_value({"crew": "Gilligan", 'position': "mate"})


def test_null_forbidden():
    col = Column('employeeid', null=False)
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'employeeid': None})


def test_other_nulls():
    col = FloatColumn(name='bogus2', null=False)
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'bogus2': np.nan})


@pytest.mark.parametrize('value', ["", " ", "\t"])
def test_blank_forbidden(value):
    col = Column(name='not_blank', null=False, blank=False)
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'not_blank': value})


def test_default_value():
    col = Column('location', default='HQ')
    input = [{'location': 'Atlanta'}, {'location': None}]
    data = [col.check_and_cast_value(row) for row in input]
    for row in data:
        if row[col.name] is None:
            assert col.fix_value(row[col.name]) == 'HQ'
        else:
            assert col.fix_value(row[col.name]) == row[col.name]


def test_nan_can_be_default():
    col = FloatColumn(name="Warp speed", default=np.nan)
    value = col.fix_value(None)
    assert isinstance(value, float)

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


def test_order_of_allowed_value_checking():
    col = Column('sale_type',
                 fix_value_fn='capitalize',
                 allowed_values=['Reg', 'Final', 'Exchange']
                 )
    assert col.check_and_cast_value({'sale_type': "reg"}) == {'sale_type': "Reg"}


def test_empty_allowed_values():
    col1 = Column('forbidden_field', allowed_values=[])
    assert col1.check_and_cast_value({'forbidden_field': None}) == {'forbidden_field': None}
    assert col1.check_and_cast_value({'forbidden_field': 'secret'}) == {'forbidden_field': 'secret'}
    col2 = Column('forbidden_field', allowed_values=[None])
    with pytest.raises(DataErrorException):
        col2.check_and_cast_value({'forbidden_field': ''})
    col3 = Column('forbidden_field', allowed_values=[''])
    with pytest.raises(DataErrorException):
        col3.check_and_cast_value({'forbidden_field': None})


def test_allowed_values_cast_to_array():
    col1 = Column(name='only-one', allowed_values='highlander')
    col1.check_and_cast_value({'only-one': 'highlander'})


def test_allowed_values_cast_int_to_array():
    col2 = IntColumn(name='answer', allowed_values=42)
    col2.check_and_cast_value({'answer': '42'})


def test_fix_and_cast_value():
    col = Column(name='status', fix_value_fn='capitalize')
    assert col.check_and_cast_value({'status': 'active'}) == {'status': 'Active'}


def test_fix_and_cast_value_with_allowed_values():
    col = Column(name='status', fix_value_fn='capitalize', allowed_values=['Active', 'Inactive'])
    assert col.check_and_cast_value({'status': 'active'}) == {'status': 'Active'}
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'status': 'unknown'})


def test_fix_and_cast_value_with_null_forbidden():
    col = Column(name='status', null=False, fix_value_fn='title')
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'status': None})


def test_fix_and_cast_value_with_default():
    col = Column(name='status', default='active', fix_value_fn='capitalize')
    assert col.check_and_cast_value({'status': None}) == {'status': 'Active'}


def test_int_column_fix_and_cast_value():
    col = IntColumn(name='quantity', fix_value_fn='abs')
    assert col.check_and_cast_value({'quantity': '-10'}) == {'quantity': 10}


def test_float_column_fix_and_cast_value():
    col = FloatColumn(name='amount', fix_value_fn='abs')
    assert col.check_and_cast_value({'amount': '-123.45'}) == {'amount': 123.45}


def test_date_column_strips_spaces():
    col = DateColumn(name='date')
    assert col.check_and_cast_value({'date': ' 2023-01-01 '}) == {'date': date(2023, 1, 1)}


def test_datetime_column_strips_spaces():
    col = DateTimeColumn(name='timestamp')
    assert col.check_and_cast_value({'timestamp': ' 2023-01-01 12:34 '}) == {'timestamp': datetime(2023, 1, 1, 12, 34)}


def test_check_and_cast_value_with_custom_callable():
    def custom_fix(value):
        return value.strip().upper()

    col = Column(name='code', fix_value_fn=custom_fix, allowed_values=['ABC', 'XYZ'])
    assert col.check_and_cast_value({'code': ' abc '}) == {'code': 'ABC'}
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'code': ' unknown '})


def test_check_and_cast_value_with_multiple_fix_functions():
    col = Column(name='code', fix_value_fn=['strip', 'upper'], allowed_values=['ABC', 'XYZ'])
    assert col.check_and_cast_value({'code': ' abc '}) == {'code': 'ABC'}
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'code': ' unknown '})


def test_check_and_cast_value_with_minmax():
    col = IntColumn(name='age', min_value=18, max_value=65)
    assert col.check_and_cast_value({'age': '30'}) == {'age': 30}
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'age': '17'})
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'age': '66'})


def test_check_and_cast_value_with_on_error():
    col = Column(name='category', allowed_values=['A', 'B'], on_error=ON_ERROR_DROP_ROW)
    with pytest.raises(DropRowException):
        col.check_and_cast_value({'category': 'C'})


def test_column_can_drop_row_with_fix_function():
    def drop_if_no_value(value):
        if value is None:
            raise DropRowException("Value is required")
        return value

    col = Column(name='required_field', fix_value_fn=drop_if_no_value)
    with pytest.raises(DropRowException):
        col.check_and_cast_value({'required_field': None})


# Parametrized tests to cover more scenarios

@pytest.mark.parametrize('value,expected', [
    ('1', True), ('0', False), ('yes', True), ('no', False), ('', None)
])
def test_boolean_column_variants(value, expected):
    col = BooleanColumn(name='is_active')
    assert col.check_and_cast_value({'is_active': value}) == {'is_active': expected}


@pytest.mark.parametrize('value,expected', [
    ('42', 42), (' 42 ', 42), ('42.0', 42)
])
def test_int_column_variants(value, expected):
    col = IntColumn(name='quantity')
    assert col.check_and_cast_value({'quantity': value}) == {'quantity': expected}


@pytest.mark.parametrize('value,expected', [
    ('42.0', 42.0), (' 42.0 ', 42.0), ('42', 42.0)
])
def test_float_column_variants(value, expected):
    col = FloatColumn(name='amount')
    assert col.check_and_cast_value({'amount': value}) == {'amount': expected}


@pytest.mark.parametrize('value,expected', [
    ('2023-01-01', date(2023, 1, 1)), (' 20230101 ', date(2023, 1, 1)), ('2023/01/01', date(2023, 1, 1))
])
def test_date_column_variants(value, expected):
    col = DateColumn(name='date')
    assert col.check_and_cast_value({'date': value}) == {'date': expected}


# Test naming features


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


def test_conflicting_renames():
    with pytest.raises(PhaserError):
        Phase(columns=[FloatColumn(name="Division", rename='div'), IntColumn(name="Divisor", rename='div')])


def test_conflicting_canonicalized_renames():
    # Unlike test_conflicting_renames which can be detected when the Phase is declared, this can only be detected
    # when the data is loaded and compared to the column renames.
    phase = Phase(columns=[Column(name="Fn", rename="Function")])
    phase.load_data([{'fn': "Counsellor", "FN": "Deanna Troy"}, {'fn': "First Officer", "FN": "William Riker"}])
    with pytest.raises(PhaserError):
        phase.rename_columns()


def test_column_names_with_case_difference():
    # This test confirms existing behavior, although we could change our minds about it.  The current behavior is
    # that if two columns have different capitalization in the data, even though the user is not trying to rename them,
    # we raise an error.
    phase = Phase(columns=[Column(name="FN"), Column(name="fn")])
    phase.load_data([{'fn': "Counsellor", "FN": "Deanna Troy"}, {'fn': "First Officer", "FN": "William Riker"}])
    with pytest.raises(PhaserError):
        phase.rename_columns()


@pytest.mark.parametrize('column_name', ['country of origin', 'country_of_origin', 'Country of\nOrigin'])
def test_canonicalize_names(column_name):
    col1 = Column("Country of Origin")
    phase = Phase(columns=[col1])
    phase.load_data([{column_name: 'UK'}, {column_name: 'US'}])
    phase.rename_columns()
    assert all(list(row.keys()) == ['Country of Origin'] for row in phase.row_data)
    assert phase.headers == ['Country of Origin']


@pytest.mark.parametrize('bad_name', ['1\n2\n3', 'a\tb\tc', None, np.nan, 5.8, "     "])
def test_forbidden_column_name_characters(bad_name):
    with pytest.raises(PhaserError):
        Column(bad_name)


def test_strip_column_name_spaces(tmpdir):
    phase = Phase()
    phase.load_data([{' id': '1', ' name': 'James T. Kirk'}])
    phase.rename_columns()
    assert phase.row_data[0]['id'] == '1'


def test_curious_quote_situation(tmpdir):
    """ If we load column names in with clevercsv or pandas, column names in quotes get the quotes removed
     so "id","name" loads in the names you'd expect.  However, we can't count on that working if there are ALSO
     spaces around column names - which normal people quite normally add when they're trying to 'fix' a CSV.
     This test provides what the libraries provide after opening a CSV where the opening line is
         "id", "name"\n
     to make sure that our code strips the spaces AND quotes."""
    phase = Phase()
    phase.load_data([{'id': '1', ' "name"': 'James T. Kirk'}])
    phase.rename_columns()
    assert phase.row_data[0]['name'] == 'James T. Kirk'


# Testing BooleanColumn

boolean_tests = [
    ("T", True),
    ("t", True),
    ("True", True),
    ("Yes", True),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize("value,cast_value", boolean_tests)
def test_boolean_column_casts(value, cast_value):
    assert BooleanColumn("test").cast(value) == cast_value


def test_boolean_required():
    phase = Phase(columns=[BooleanColumn("test", required=True)])
    phase.load_data([{'id': 1}])
    with pytest.raises(DataErrorException):
        phase.do_column_stuff()


def test_column_not_set_if_not_required_and_not_saved():
    # Making this work -- not add the column at the beginning of the phase only to drop it at the end -- allows
    # a phase to flatten JSON columns and then not save some of the flattened results at the end of the phase.
    # E.g. without this, a phase can't have both "Column('payload__content', required=False, save=False)" and a step
    # like "flatten_column('payload')", because the flatten method fails when the payload__content was added during
    # column logic handling.

    phase = Phase(columns=[Column("test", required=False, save=False)])
    phase.load_data([{'id': 1}])
    phase.do_column_stuff()
    assert 'test' not in phase.row_data[0].keys()


def test_column_cast_if_not_required_and_not_saved_but_is_there():
    # This is the flip side of the logic tested in test_column_not_set_if_not_required_and_not_saved - if the column
    # is not saved at the end, and not required to be there, but it *might* be there and it *might* be used,
    # we want to operate on the column as normal rather than skip it in 'cast_each_column_value'.
    phase = Phase(columns=[IntColumn("test", required=False, save=False)])
    phase.load_data([{'id': 1, 'test': "5"}, {'id': 2}])
    phase.do_column_stuff()
    assert phase.row_data[0]['test'] == 5
    assert 'test' not in phase.row_data[1].keys()


def test_boolean_not_null():
    with pytest.raises(DataErrorException):
        BooleanColumn('test', null=False).check_and_cast_value({'id': 1, 'test': None})


def test_boolean_null():
    # Should not raise an exception
    BooleanColumn('test', null=True).check_and_cast_value({'id': 1, 'test': None})

# Testing IntColumn


def test_int_column_casts():
    col = IntColumn(name="Age", min_value=0)
    phase = Phase(columns=[col])
    phase.load_data([{'age': "3"}, {'age': "4 "}, {'age': "5.0"}])
    phase.do_column_stuff()
    assert [row['Age'] for row in phase.row_data] == [3, 4, 5]


def test_int_column_doesnt_need_to_cast():
    col = IntColumn(name="Age")
    assert col.cast(1) == 1


def test_int_column_null_value():
    col = IntColumn(name="Age")
    assert col.cast(None) is None
    assert col.cast(np.nan) is None


def test_cast_nans_and_nones():
    col = IntColumn(name="Skill level")
    assert col.cast(np.nan) is None
    assert col.cast("NULL") is None
    assert col.cast(None) is None
    assert col.cast("") is None
    col = FloatColumn(name="Warp speed")
    assert col.cast(np.nan) is None


def test_cast_when_not_present():
    col = IntColumn(name="Shoe size", required=False)
    phase = Phase(columns=[col])
    phase.load_data([{'TShirt size': "medium"}, {'Hat size': 21}])
    phase.do_column_stuff()
    assert all([row["Shoe size"] is None for row in phase.row_data])


def test_int_column_minmax():
    col = IntColumn(name="Age", min_value=0, max_value=130)
    with pytest.raises(DataErrorException):
        col.check_value(-1)
    with pytest.raises(DataErrorException):
        col.check_value(2000)


def test_float_column():
    pay = FloatColumn(name="pay", min_value=0.01, rename="payRate", required=True)
    phase = Phase(columns=[pay])
    phase.load_data([{'payRate':'12345.0'} ])
    phase.do_column_stuff()
    assert phase.row_data[0]['pay'] == 12345.0
    assert isinstance(phase.row_data[0]['pay'], float)

# Testing Datetime and date column

def test_datetime_column_casts():
    col = DateTimeColumn(name="start", datetime_format="%Y/%m/%d %H:%M")
    with pytest.raises(Exception):
        col.cast('Data')
    assert col.cast("2223/01/01 14:28") == datetime(2223,1,1, 14, 28)


def test_datetime_column_doesnt_need_to_cast():
    col = DateTimeColumn(name="start")
    sample_dt = datetime(2023, 1, 1, 14, 14)
    assert col.cast(sample_dt) == sample_dt


def test_datetime_column_casts_consistently():
    # This test was originally intended to make sure that '1/31/2024' and '31/1/2024' don't get cast to the
    # same value - which would happen if we used dateutil to individually parse each date.  If a column has
    # both values in its data, something is wrong and it's better to know that about the data than to hide it.
    # If a column has both 1/31/2024 and 31/1/2024, we don't know how to parse '5/6/2024'.  Now that we use a
    # more rigorous date parsing logic, the comparison of the results of 1/31/2024 and 31/1/2024 is a little bogus,
    # because one of those two raises a ValueError.
    col = DateTimeColumn(name="logtime", datetime_format='%m/%d/%Y')
    assert col.cast('5/6/2024') != col.cast('6/5/2024')
    try:
        assert col.cast('31/1/2024') != col.cast('1/31/2024')
    except Exception:
        pass


def test_date_column_custom_format():
    col=DateColumn(name="stardate", date_format="%Y|%m|%d")
    assert col.cast("2223|01|01") == date(2223,1,1)


def test_datetime_column_applies_tz():
    col = DateTimeColumn(name="start", default_tz=gettz("America/Los Angeles"))
    value = col.cast("22230101")
    assert value.tzname() == "PST"


def test_date_column_casts_to_date():
    col = DateColumn(name="start")
    value = col.cast("2223/01/01")
    assert value == date(2223,1,1)
    assert hasattr(value, 'tzname') is False


def test_date_column_doesnt_need_to_cast():
    col = DateColumn(name="start")
    assert col.cast(date(2023,1,1)) == date(2023,1,1)


def test_date_column_converts_time_to_date():
    col = DateColumn(name="start")
    assert col.cast("2023-01-01T10:50") == date(2023, 1, 1)


def test_date_column_range():
    col = DateColumn(name="start", min_value=date(2019, 12, 1), max_value=date.today())
    col.check_and_cast_value({'start': "2024-01-14"})
    with pytest.raises(DataErrorException):
        col.check_and_cast_value({'start': "2012-01-01"})


# Tests of column error handling


def test_column_error_selection():
    col = Column(name='room', allowed_values=['stateroom', 'cabin'], on_error=ON_ERROR_DROP_ROW)
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
    assert phase.context.phase_has_errors(phase.name) is False
    assert len(phase.row_data) == 1
    assert phase.row_data[0]['Shoe size'] == 42
