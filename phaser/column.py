from datetime import datetime
from dateutil.parser import parse
from decimal import Decimal
import inspect
import types
from collections.abc import Iterable


class Column:
    """ Default Column class including for columns of Strings """
    FORBIDDEN_COL_NAME_CHARACTERS = ['\n', '\t']

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None):
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
        """
        self.name = str(name).strip()
        assert all(character not in name for character in Column.FORBIDDEN_COL_NAME_CHARACTERS)
        self.required = required
        self.null = null
        self.default = default
        self.fix_value_fn = fix_value_fn
        self.rename = rename or []
        self.allowed_values = allowed_values

        if self.null is False and self.default is not None:
            raise Exception(f"Column {self.name} defined to error on null values, but also provides a non-null default")

    def check_and_cast(self, headers, data):
        """
        Checks a dataset to make sure the conditions put on this column are met, and casts to another data type if
        appropriate. Columns might need to do some checking, then some casting, then more checking,
        to complete their work.  However, users who want to subclass Column to do a special kind of column
        (e.g. ISBNNumberColumn) ought to need to only override check_value or cast.
        :param headers: just the column headers found in data, for checking presence and fixing case
        :param data: all of the batch data in list(dict) format
        :return: None
        """
        # LMDTODO: This may significantly change structure when we introduce error handling.  Also it may violate
        # expectations that one column's logic not only casts its values, it also drops rows - so the next column
        # receives fewer rows.  I still think that's right for a data cleaning library but need to check this lots.
        if self.required:
            if self.name not in headers:
                raise Exception(f"Header {self.name} not found in {headers}")
        new_rows = []
        for row in data:
            value = row[self.name]
            if self.null is False and value is None:
                # Checking for null values comes before casting
                raise ValueError(f"Null value found in column {self.name}")

            new_value = self.cast(value)   # Cast to another datatype (int, float) if subclass
            self.check_value(new_value)    # More checking comes after casting
            new_value = self.fix_value(new_value)
            row[self.name] = new_value
            new_rows.append(row)
        return new_rows

    def cast(self, value):
        """ Basic column does no casting. Override this method in a subclass to cast to other things besides strings """
        return value

    def check_value(self, value):
        """ Raises ValueError if something is wrong with a value in the column.  ValueError will be trapped by Phase
        to try to apply the appropriate error handling.  Override this (don't forget to call super().check_value() """
        if self.allowed_values and not (value in self.allowed_values):
            raise ValueError(f"Column {self.name} had value {value} not found in allowed values")

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


class IntColumn(Column):

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None,
                 min_value=None,
                 max_value=None):
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
        :param min_value: If data is below this value, column raises errors
        :param max_value: If data is above this value, column raises errors
        """
        super().__init__(name,
                         required=required,
                         null=null,
                         default=default,
                         fix_value_fn=fix_value_fn,
                         rename=rename,
                         allowed_values=allowed_values)
        self.min_value = min_value
        self.max_value = max_value

    def check_value(self, value):
        super().check_value(value)
        if self.min_value is not None and (value < self.min_value):
            raise ValueError(f"Value for {self.name} is {value}, less than min {self.min_value}")
        if self.max_value is not None and (value > self.max_value):
            raise ValueError(f"Value for {self.name} is {value}, more than max {self.max_value}")

    def cast(self, value):
        if value is None:
            return None
        return int(Decimal(value))


class DateTimeColumn(Column):

    def __init__(self,
                 name,
                 required=True,
                 null=True,
                 default=None,
                 fix_value_fn=None,
                 rename=None,
                 allowed_values=None,
                 min_value=None,
                 max_value=None,
                 date_format_code=None,
                 default_tz=None):
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
        :param min_value: If data is below this value, column raises errors
        :param max_value: If data is above this value, column raises errors
        :param date_format_code:  Formatting string used by datetime.strptime to parse string to date,
            e.g. '%d/%m/%y %H:%M:%S.%f', '%d/%m/%Y' or '%m/%d/%y'.  If left None, class will use dateutil.parser.
        :param default_tz: If timezone is not specified in value, assume this timezone applies.
        """
        super().__init__(name,
                         required=required,
                         null=null,
                         default=default,
                         fix_value_fn=fix_value_fn,
                         rename=rename,
                         allowed_values=allowed_values)
        self.min_value = min_value
        self.max_value = max_value
        self.date_format_code = date_format_code
        self.default_tz = default_tz

    def check_value(self, value):
        super().check_value(value)
        if self.min_value is not None and (value < self.min_value):
            raise ValueError(f"Value for {self.name} is {value}, less than min {self.min_value}")
        if self.max_value is not None and (value > self.max_value):
            raise ValueError(f"Value for {self.name} is {value}, more than max {self.max_value}")

    def cast(self, value):
        if value is None:
            return None
        if self.date_format_code:
            value = datetime.strptime(value, self.date_format_code)
        else:
            value = parse(value)
        if value.tzname() is None and self.default_tz is not None:
            value  = value.replace(tzinfo=self.default_tz)
        return value

class DateColumn(DateTimeColumn):
    def cast(self, value):
        value = super().cast(value)
        return datetime.date(value)


# -------  Below here: not exported for user use  -------

def make_strict_name(name):
    # LMDTODO: making a field name 'strict' should also condense multiple space characters to one
    return name.lower().replace('_', ' ')


def call_method_on(obj, method):
    # LMDTODO Move to a utils location?
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
