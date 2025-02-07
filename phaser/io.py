import csv
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
import logging
import math
from phaser.exceptions import DataErrorException, PhaserError
import json

logger = logging.getLogger(__name__)
EXTRA_FIELDS_KEY = "__phaser_extra_fields__"
MISSING_FIELD_VAL = "__phaser_missing_field__"


def read_json(source, format=format):
    """
    This read_json helper assumes pandas style orient='records' format.  If your data uses something else,
    a custom Pipeline can have a custom read/save implementation.
    :param source:
    :return: data in python list-of-dicts format.
    """
    with open(source, 'r') as json_file:
        data = json.load(json_file)
        if isinstance(data, list):
            return data
        else:
            raise PhaserError("JSON data file format expected to hold a list of dict records - did not find list.")


def save_json(filename, row_data):
    with open(filename, 'w') as f:
        json.dump(row_data, f)


def read_csv(source, delimiter=','):
    data = []
    with open(source, 'r', newline="") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        first_line = next(drop_empty_rows(csv_reader))
        while first_line == [] or first_line[0].startswith('#'):
            first_line = next(csv_reader)
        if len(first_line) > len(set(first_line)):
            raise DataErrorException(f"CSV {source} has duplicate column names and cannot reliably be parsed")
        dict_reader = csv.DictReader(drop_empty_rows(csv_file),
                                     fieldnames=first_line,
                                     restkey=EXTRA_FIELDS_KEY,
                                     restval=MISSING_FIELD_VAL,
                                     delimiter=delimiter)
        for row in dict_reader:
            if EXTRA_FIELDS_KEY in row.keys() and is_list_empty(row[EXTRA_FIELDS_KEY]):
                row.pop(EXTRA_FIELDS_KEY)
            if MISSING_FIELD_VAL in row.values():
                raise Exception(f"Fields missing in record <{row}>")
            if len(row) != len(first_line):
                raise Exception(f"Inconsistent # of fields ({len(row)}) detected first in record <{row}>")
            if all(value == '' for value in row.values()):
                logger.debug("Row with all empty values dropped from CSV")
            else:
                data.append(row)

    return data


def drop_empty_rows(csv_stream):
    for row in csv_stream:
        if is_list_empty(row):
            # Skipping rows that are empty or nothing but commas - often generated at the end of Excel tables
            continue
        yield row


def is_list_empty(value):
    """
    >>> is_list_empty('')
    True
    >>> is_list_empty([])
    True
    >>> is_list_empty([''])
    True
    >>> is_list_empty(['  '])
    True
    """
    return (value == []
            or isinstance(value, list) and all([is_list_empty(i) for i in value])
            or isinstance(value, str) and value.strip() == '')


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

def save_csv(filename, row_data, fieldnames=None):
    if not row_data:
        return

    iterator = FixNansIterator(row_data)
    try:
        first = next(iterator)
    except StopIteration:
        return

    if fieldnames is None:
        fieldnames = list(first.keys())
    try:
        with open(filename, "w", newline="") as fp:
            w = csv.DictWriter(fp, fieldnames=fieldnames)
            w.writeheader()
            w.writerow(first)
            w.writerows(iterator)
    except ValueError:
        all_fieldnames = set()
        [all_fieldnames.update(row.keys()) for row in row_data]
        extra_fieldnames = [name for name in all_fieldnames if name not in fieldnames]
        save_csv(filename, row_data, fieldnames=all_fieldnames)
        logger.info("Data had added fields in some rows, which were duplicated across all rows to save as valid CSV. " +
                    "If this is not the desired behavior, options include: save as JSON, mark extra fields as " +
                    "not-saved in Pipeline, or set all fields explicitly on all rows.  (Added fields found: """ +
                    ', '.join(extra_fieldnames) + ')')


class SavableObject():
    """ Base class for data that can be saved as tabular data - but can reorganize data coming in or out"""
    def __init__(self, name, data_type, data=None):
        self.name = name
        self.data_type = data_type
        if data and not isinstance(data, data_type):
            raise PhaserError(f"{self.__class__} initialized with wrong data structure. Must be a {data_type}")
        self.data = data
        self.to_save = False

    def load_data(self, data):
        """ Load data from the source, overwriting any data that may be stored
        in the object already. The source should be in a format that was written
        by the save function."""
        self.data = data

    def prepare_for_save(self):
        """ Save data to the destination in a format that is appropriate to be
        read back in by the load function."""
        return self.data


class ExtraRecords(SavableObject):
    """ A holder of data that either comes from an extra source is will be added
    to as an extra output. Its data is in the form of a sequence, most likely
    a list of dicts and represents data that is meant to be appended to or
    processed over."""
    def __init__(self, name, data=None):
        super().__init__(name, Sequence, data)


class ExtraMapping(SavableObject):
    """ A holder of data that either comes from an extra source is will be added
    to as an extra output. Its data is in the form of a mapping, most likely
    a dict and represents data that is meant to be accessed by a key."""
    def __init__(self, name, data=None):
        super().__init__(name, Mapping, data)

    def load_data(self, tabular_data):
        self.data = {
            row['key']: row['value'] for row in tabular_data
        }

    def prepare_for_save(self):
        if self.data:
            return [
                { 'key': key, 'value': value }
                for key, value in self.data.items()
            ]
