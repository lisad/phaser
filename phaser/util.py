import pandas as pd


def read_csv(source):
    """ Includes the default settings phaser uses with panda's read_csv. Can be overridden to provide
    different read_csv settings.
    Defaults:
    * assume all column values are strings, so leading zeros or trailing zeros don't get destroyed.
    * assume ',' value-delimiter
    * skip_blank_lines=True: allows blank AND '#'-led rows to be skipped and still find header row
    * doesn't use indexing
    * does attempt to decompress common compression formats if file uses them
    * assume UTF-8 encoding
    * uses '#' as the leading character to assume a row is comment
    * Raises errors loading 'bad lines', rather than skip
    """
    return pd.read_csv(source,
                       dtype='str',
                       sep=',',
                       skip_blank_lines=True,
                       index_col=False,
                       comment='#')
