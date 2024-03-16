from clevercsv import stream_dicts, stream_table
from .exceptions import DataErrorException


def read_csv(source):
    stream = stream_table(source)
    header = next(stream)
    if len(header) > len(set(header)):
        raise DataErrorException(f"CSV {source} has duplicate column names and cannot reliably be parsed")

    return list(stream_dicts(source))
