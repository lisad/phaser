import math
from clevercsv import stream_dicts, stream_table, DictWriter
from phaser.exceptions import DataErrorException


def read_csv(source):
    # Using the 'stream_table' method we can get just the header row, and perform our check for
    # duplicate columns before having the library parse everything into dicts.
    stream = stream_table(source)
    header = next(stream)
    if len(header) > len(set(header)):
        raise DataErrorException(f"CSV {source} has duplicate column names and cannot reliably be parsed")

    return list(stream_dicts(source))


class FixNansIterator:
    """
    If NaN objects from numpy or pandas operations get left in data values, saving the file would include 'nan'
    and reloading the file (not just in phaser but in other programs) can cause errors.  For now, we replace with
    None, but we could make this configurable so a user could output "!ERROR" or something
    >>> list(FixNansIterator([{'val': float('nan')}]))[0]
    {'val': None}

    """
    def __init__(self, list_of_dicts):
        self._sequence = list_of_dicts
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index < len(self._sequence):
            row = self._sequence[self._index]
            self._index += 1
            for key, val in row.items():
                if safe_is_nan(val):
                    row[key] = None
            return row
        else:
            raise StopIteration


def safe_is_nan(value):
    """
    This function needs to be directly testable in case we find a better way than try/except that doesn't use
    pandas isnull or doesn't attempt to replace too many things with none.  The test may fail if numpy isn't
    available - numpy needs to be a test dependency but not a library dependency.
    >>> safe_is_nan('a')
    False
    >>> safe_is_nan(1)
    False
    >>> import numpy as np
    >>> safe_is_nan(np.nan)
    True
    >>> safe_is_nan(float('nan'))
    True
    """
    try:
        if math.isnan(value):
            return True
        return False
    except TypeError:
        return False


def is_nan_or_null(value):
    """
    NOTE: For purposes of dealing with IO, special values that are sometimes saved for None are also treated as None.
    >>> is_nan_or_null(None)
    True
    >>> is_nan_or_null(True)
    False
    >>> is_nan_or_null("NULL")
    True
    """
    return value is None or safe_is_nan(value) or value in ["NULL", "None"]


def is_empty(value):
    """
    >>> is_empty("   ")
    True
    >>> is_empty("\t ")
    True
    >>> is_empty("Not empty")
    False
    """
    if isinstance(value, str):
        return value.replace('\t', '').replace('\n', '').replace(' ', '') == ""
    return False

def save_csv(filename, row_data):
    if not row_data:
        return

    iterator = FixNansIterator(row_data)
    try:
        first = next(iterator)
    except StopIteration:
        return

    fieldnames = list(first.keys())
    with open(filename, "w", newline="") as fp:
        w = DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        w.writerow(first)
        w.writerows(iterator)