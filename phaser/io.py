import pandas as pd
from clevercsv import stream_dicts, stream_table
from clevercsv.wrappers import write_dicts
from phaser.exceptions import DataErrorException


def read_csv(source):
    # Using the 'stream_table' method we can get just the header row, and perform our check for
    # duplicate columns before having the library parse everything into dicts.
    stream = stream_table(source)
    header = next(stream)
    if len(header) > len(set(header)):
        raise DataErrorException(f"CSV {source} has duplicate column names and cannot reliably be parsed")

    return list(stream_dicts(source))


def save_csv(filename, row_data):
    write_dicts(row_data, filename)
