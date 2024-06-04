from datetime import datetime
from dateutil.parser import parse
from decimal import Decimal
import inspect
import logging
import types
from collections.abc import Iterable
from .exceptions import DropRowException, DataErrorException, WarningException, PhaserError
from .constants import ON_ERROR_STOP_NOW, ON_ERROR_COLLECT, ON_ERROR_WARN, ON_ERROR_DROP_ROW
from .io import is_nan_or_null, safe_is_nan, is_empty

logger = logging.getLogger('phaser')


""" Contains definitions of columns that can apply certain rules and datatypes to values automatically.

Column: Use for strings & general purpose
IntColumn, FloatColumn: Cast to int/float datatypes and apply number logic like min_value, max_value
DateColumn, DateTimeColumn: Cast to date/time datatypes and apply calendar logic like default timezone
BooleanColumn: cast 1, 0, T, F, yes, no, TRUE, FALSE etc to python True and False values.

"""


class Column:
    """
    Sets up a Column instance ready to do type, format, null and default checking on values, as well as
    renaming the column name itself to chosen version.  This column is useful for fields with Strings as values.

    :param name: The preferred name/presentation of the column, e.g. "Date of Birth" or "first_name"
    :param required: If the column is required, the phase will present errors if it is missing.
    :param null: Checks all values of the column for null values to raise as errors.
    :param default: A default value to apply if a column value is null. Not compatible with "null=False"
    :param fix_value_fn: A function (string or callable) or array of functions to apply to each value
    :param rename: A set of names that may be used in the data as column headers, all of which should be mapped to
        the preferred name of this column. Upon loading the data, all rows that have columns matching
        any alternate name in this set will have a column with the preferred name with the same data in
        it. In other words, any data in a column name in `rename` will end up in a column named `name`.
    :param allowed_values: If allowed_values is not empty and a column value is not in the list, raises errors.
    :param save: if True, column is saved at the end of the phase; if not it is omitted.
    :param on_error: Choose from 'warn', 'drop_row', 'collect', 'stop_now' to pick how errors checking or
        fixing this column affect the pipeline.
    """
    FORBIDDEN_COL_NAME_CHARACTERS = ['\n', '\t']

    ON_ERROR_VALUES = {
        ON_ERROR_WARN: WarningException,
        ON_ERROR_DROP_ROW: DropRowException,
        ON_ERROR_COLLECT: DataErrorException,
        ON_ERROR_STOP_NOW: Exception
    }

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 blank=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None,
                 save=True,
                 on_error=None):
        self.name = str(name).strip()
        assert all(character not in name for character in Column.FORBIDDEN_COL_NAME_CHARACTERS)
        self.required = required
        self.null = null
        self.blank = blank
        self.default = default
        self.fix_value_fn = fix_value_fn
        self.rename = rename or []
        if isinstance(self.rename, str):
            self.rename = [self.rename]
        self.allowed_values = allowed_values
        self.save = save
        self.use_exception = DataErrorException
        if on_error and on_error not in Column.ON_ERROR_VALUES.keys():
            raise PhaserError(f"Supported on_error values are [{', '.join(Column.ON_ERROR_VALUES.keys())}]")
        if on_error:
            self.use_exception = Column.ON_ERROR_VALUES[on_error]

        if self.null is False and self.default is not None:
            raise PhaserError(f"Column {self.name} defined to error on null values, but also provides a non-null default")

    def check_required(self, data_headers):
        """
        If this column is required, then checks the list of headers of the dataset to see if its name is there.

        :param data_headers: just the column headers found in data
        :return: None
        """
        if self.required:
            if self.name not in data_headers:
                raise self.use_exception(f"Header {self.name} not found in {data_headers}")

    def check_and_cast_value(self, row):
        """ This checks to see if the value is there before attempting to cast it.  It does some checks before
        casting the value to a datatype, and some other checks afterward. .  Most of the time, a custom
        algorithm for converting a value to a specific datatype can just override the simpler 'cast' method.

        :param row: entire row is passed for simplicity elsewhere and in case this needs more scope
        """
        value = row.get(self.name)
        if self.null is False and value is None:
            raise self.use_exception(f"Null value found in column {self.name}")

        new_value = self.cast(value)   # Cast to another datatype (int, float) if subclass

        self.check_value(new_value)
        fixed_value = self.fix_value(new_value)
        if fixed_value is None and new_value is not None:
            logger.debug(f"Column {self.name} set value to None while fixing value")
        row[self.name] = fixed_value
        return row

    def cast(self, value):
        """ Basic column only fixes NaN values. Even values like "NULL" or "None" might be actual values if we
        don't know the type. this is good to subclass to cast python data types or custom objects.
        """
        if safe_is_nan(value):
            return None
        return value

    def check_value(self, value):
        """ Raises chosen exception type if something is wrong with a value in the column.
            One can override this to use a different exception or check value in a different way
            (don't forget to call super().check_value() """
        if not self.blank and not value.strip():  # Python boolean casting returns false if string is empty
            raise self.use_exception(f"Column `{self.name}' had blank value")
        if self.allowed_values and not (value in self.allowed_values):
            raise self.use_exception(f"Column '{self.name}' had value {value} not found in allowed values")

    def fix_value(self, value):
        """ Sets value to default if provided and appropriate, and calls any functions or
        methods passed as 'fix_value_fn'.  Also, this method can be overridden if a special column
        has a custom way to fix values and it's worth subclassing Column - just be sure to call
        value = super(value) if you want to apply the logic already here.
        """
        if value is None and self.default is not None:
            value = self.default
        if self.fix_value_fn:
            if not isinstance(self.fix_value_fn, Iterable) or isinstance(self.fix_value_fn, str):
                # Strings are iterables in python, yet we don't want to break up a string into letters
                self.fix_value_fn = [self.fix_value_fn]
            for fn in self.fix_value_fn:
                value = call_method_on(value, fn)
        return value


class BooleanColumn(Column):
    """
    Validates truthy and falsey values, as defined in TRUE_VALUES and FALSE_VALUES.
    """

    TRUE_VALUES = ['t', 'true', '1', 'yes', 'y']
    FALSE_VALUES = ['f', 'false', '0', 'no', 'n']

    def __init__(self,
                 name,
                 required=True,
                 null=False,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 save=True,
                 on_error=None):
        super().__init__(name,
                         required=required,
                         null=null,
                         blank=True,   # Ignores blank parameter - only null=T/F applies
                         default=default,
                         fix_value_fn=fix_value_fn,
                         rename=rename,
                         allowed_values=None,
                         save=save,
                         on_error=on_error)

    def cast(self, value):
        if is_nan_or_null(value) or is_empty(value):
            return None
        if value.lower() in BooleanColumn.TRUE_VALUES:
            return True
        if value.lower() in BooleanColumn.FALSE_VALUES:
            return False
        raise self.use_exception(f"Value {value} not recognized as a boolean value")


class IntColumn(Column):
    """
    Sets up a Column instance ready to do type, format, null and default checking on values, as well as
    renaming the column name itself to chosen version.

    :param name: The preferred name/presentation of the column, e.g. "Date of Birth" or "first_name"
    :param required: If the column is required, the phase will present errors if it is missing.
    :param null: Checks all values of the column for null values to raise as errors.
    :param default: A default value to apply if a column value is null. Not compatible with "null=False"
    :param fix_value_fn: A function (string or callable) or array of functions to apply to each value
    :param rename: A set of names that may be used in the data as column headers, all of which should be mapped to
        the preferred name of this column. Upon loading the data, all rows that have columns matching
        any alternate name in this set will have a column with the preferred name with the same data in
        it. In other words, any data in a column name in `rename` will end up in a column named `name`.
    :param allowed_values: If allowed_values is not empty and a column value is not in the list, raises errors.
        To supply a range, use min_value and max_value instead.  NOTE: this is checked after casting,
        so to check allowed values of a column specified to cast to int, such as IntColumn, check for
        values like [1, 2, 3] rather than ["1", "2", "3"]
    :param save: if True, column is saved at the end of the phase; if not it is omitted.
    :param min_value: If data is below this value, column raises errors
    :param max_value: If data is above this value, column raises errors
    """

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None,
                 save=True,
                 on_error=None,
                 min_value=None,
                 max_value=None):
        super().__init__(name,
                         required=required,
                         null=null,
                         blank=True,  # Ignores this column - only null=T/F matters
                         default=default,
                         fix_value_fn=fix_value_fn,
                         rename=rename,
                         allowed_values=allowed_values,
                         save=save,
                         on_error=on_error)
        self.min_value = min_value
        self.max_value = max_value

    def check_value(self, value):
        super().check_value(value)
        if self.min_value is not None and (value < self.min_value):
            raise self.use_exception(f"Value for {self.name} is {value}, less than min {self.min_value}")
        if self.max_value is not None and (value > self.max_value):
            raise self.use_exception(f"Value for {self.name} is {value}, more than max {self.max_value}")

    def cast(self, value):
        if is_nan_or_null(value) or is_empty(value):
            return None
        return int(Decimal(value))


class FloatColumn(IntColumn):
    """ Defines a column that accepts a float value. See `IntColumn` for parameters. """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def cast(self, value):
        if is_nan_or_null(value) or is_empty(value):
            return None
        return float(Decimal(value))


class DateTimeColumn(Column):
    """
    Sets up a DateColumn instance ready to do type, format, null and default checking on values, as well as
    renaming the column name itself to chosen version.

    :param name: The preferred name/presentation of the column, e.g. "Date of Birth" or "first_name"
    :param required: If the column is required, the phase will present errors if it is missing.
    :param null: Checks all values of the column for null values to raise as errors.
    :param default: A default value to apply if a column value is null. Not compatible with "null=False"
    :param fix_value_fn: A function (string or callable) or array of functions to apply to each value
    :param rename: A set of names that may be used in the data as column headers, all of which should be mapped to
        the preferred name of this column. Upon loading the data, all rows that have columns matching
        any alternate name in this set will have a column with the preferred name with the same data in
        it. In other words, any data in a column name in `rename` will end up in a column named `name`.
    :param allowed_values: If allowed_values is not empty and a column value is not in the list, raises errors.
        To supply a range, use min_value and max_value instead.
    :param save: if True, column is saved at the end of the phase; if not it is omitted.
    :param min_value: If data is below this value, column raises errors
    :param max_value: If data is above this value, column raises errors
    :param date_format_code:  Formatting string used by datetime.strptime to parse string to date,
        e.g. '%d/%m/%y %H:%M:%S.%f', '%d/%m/%Y' or '%m/%d/%y'.  If left None, class will use dateutil.parser.
    :param default_tz: If timezone is not specified in value, assume this timezone applies.
    """

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None,
                 save=True,
                 on_error=None,
                 min_value=None,
                 max_value=None,
                 date_format_code=None,
                 default_tz=None):
        super().__init__(name,
                         required=required,
                         null=null,
                         blank=True,  # Ignores this param - only null=T/F matters
                         default=default,
                         fix_value_fn=fix_value_fn,
                         rename=rename,
                         allowed_values=allowed_values,
                         save=save,
                         on_error=on_error)
        self.min_value = min_value
        self.max_value = max_value
        self.date_format_code = date_format_code
        self.default_tz = default_tz

    def check_value(self, value):
        """ Checks the value for every field
        Override in order to have custom error handling for example
        """
        super().check_value(value)
        if self.min_value is not None and (value < self.min_value):
            raise self.use_exception(f"Value for {self.name} is {value}, less than min {self.min_value}")
        if self.max_value is not None and (value > self.max_value):
            raise self.use_exception(f"Value for {self.name} is {value}, more than max {self.max_value}")

    def cast(self, value):
        if is_nan_or_null(value) or is_empty(value):
            return None
        if self.date_format_code:
            value = datetime.strptime(value, self.date_format_code)
        else:
            value = parse(value)
        if value.tzname() is None and self.default_tz is not None:
            value = value.replace(tzinfo=self.default_tz)
        return value


class DateColumn(DateTimeColumn):
    """ A column that supports the date value only (no time). See `DateTimeColumn` for parameters. """

    def cast(self, value):
        value = super().cast(value)
        return datetime.date(value)


# -------  Below here: not exported for user use  -------

def make_strict_name(name):
    """
    Often hand-edited spreadsheets or CSVs get extra tabs, returns or spaces in the field names
    >>> make_strict_name('Homeworld_Quadrant')
    'homeworld quadrant'
    >>> make_strict_name('Homeworld  quadrant')
    'homeworld quadrant'
    >>> make_strict_name('Homeworld\tquadrant')
    'homeworld quadrant'
    >>> make_strict_name('Homeworld \\nquadrant')
    'homeworld quadrant'
    """
    new_name = (name.lower().
                replace('_', ' ').
                replace('\t', ' ').
                replace('\n', ' '))
    return ' '.join(new_name.split())   # Replaces multiple spaces with single


def call_method_on(obj, method):
    def is_builtin_function_or_descriptor(thing):
        return isinstance(thing, (types.BuiltinFunctionType, types.BuiltinMethodType))

    if isinstance(method, str) and hasattr(obj, method):
        # Examples: value.strip(), value.lstrip(), value.rstrip(), value.lower()... or going beyond strings,
        # if the value is a date, value.weekday(), value.hour, value.year...
        result = getattr(obj, method)
        # Python has methods and builtin functions. Builtin functions like 'strip' are
        if inspect.ismethod(result) or is_builtin_function_or_descriptor(result):
            result = result()
        return result
    elif isinstance(method, str):
        # Examples: passing the value to a function like bytearray, len, date.fromisoformat, or abs. (Will it work
        # for date.fromisoformat without the right import?)
        if isinstance(obj, str):
            expression = f"{method}('{obj}')"
        else:
            expression = f"{method}({obj})"
        return eval(expression)
    elif callable(method):
        # Users will have to pass the callable rather than a string if it's not in imported scope here
        return method(obj)
    else:
        raise Exception("Case not handled - method not callable or string or attribute of obj")
