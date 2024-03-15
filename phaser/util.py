import clevercsv
from .exceptions import DataErrorException


def read_csv(source):
    stream = clevercsv.stream_table(source)
    header = next(stream)
    if len(header) > len(set(header)):
        raise DataErrorException(f"CSV {source} has duplicate column names and cannot reliably be parsed")

    return clevercsv.read_dicts(source)
