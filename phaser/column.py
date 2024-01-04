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
        :param rename: A set of alternate names for the column, so phase can rename to preferred name.
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

    def check(self, headers, data):
        """

        :param headers: just the column headers found in data
        :param data: all of the batch data in list(dict) format
        :return: None
        """
        if self.required:
            if self.name not in headers:
                raise Exception(f"Header {self.name} not found in {headers}")
        for row in data:
            value = row[self.name]
            if self.null is False and value is None:
                raise ValueError(f"Null value found in column {self.name}")
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


# -------  Not exported for user use  -------

def make_strict_name(name):
    # LMDTODO: making a field name 'strict' should also condense multiple space characters to one
    return name.lower().replace('_', ' ')


def call_method_on(obj, method):
    # LMDTODO Move to a utils location?
    def is_builtin_function_or_descriptor(thing):
        return isinstance(thing, (types.BuiltinFunctionType, types.BuiltinMethodType))

    if isinstance(method, str) and hasattr(obj, method):
        result = getattr(obj, method)
        # Python has methods and builtin functions. Builtin functions like 'strip' are
        if inspect.ismethod(result) or is_builtin_function_or_descriptor(result):
            result = result()
        return result
    elif isinstance(method, str):
        if isinstance(obj, str):
            expression = f"{method}('{obj}')"
        else:
            expression = f"{method}({obj})"
        return eval(expression)
    elif callable(method):
        return method(obj)
    else:
        raise Exception("Case not handled - method not callable or string or attribute of obj")
