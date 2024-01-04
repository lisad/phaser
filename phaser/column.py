import inspect
import types
from collections.abc import Iterable

class Column:
    """ Default Column class including for columns of Strings """
    def __init__(self, name, required=True, null=True, default=None, fix_value_fn=None, rename=None, allowed_values=None):
        self.name = str(name)
        self.required = required
        self.null = null
        self.default = default
        self.fix_value_fn = fix_value_fn
        self.rename = rename
        self.allowed_values = allowed_values

        if self.null is False and self.default is not None:
            raise Exception(f"Column {self.name} defined to error on null values, but also provides a non-null default")


    def check(self, headers, data):
        """

        :param headers: just the column headers found in data
        :param data:
        :return:
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
        if value is None and self.default is not None:
            value = self.default
        if self.fix_value_fn:
            if not isinstance(self.fix_value_fn, Iterable) or isinstance(self.fix_value_fn, str):
                # Strings are iterables in python, yet we don't want to break up a string into letters
                self.fix_value_fn = [self.fix_value_fn]
            for fn in self.fix_value_fn:
                value = call_method_on(value, fn)
        return value

def call_method_on(obj, method):
    def is_builtin_function_or_descriptor(thing):
        return isinstance(thing, (types.BuiltinFunctionType, types.BuiltinMethodType))

    if isinstance(method, str) and hasattr(obj, method):
        result = getattr(obj, method)
        # Python has methods and builtin functions. Builtin functions like 'strip' are
        if inspect.ismethod(result) or is_builtin_function_or_descriptor(result):
            result = result()
        return result
    elif isinstance(method, str):
        print(method, obj)
        if isinstance(obj, str):
            expression = f"{method}('{obj}')"
        else:
            expression = f"{method}({obj})"
        print(expression)
        return eval(expression)
    elif callable(method):
        return method(obj)
    else:
        raise Exception("Case not handled - method not callable or string or attribute of obj")