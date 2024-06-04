from .steps import batch_step
from .exceptions import DataErrorException, PhaserError
from .column import Column


def check_unique(column, strip=True, ignore_case=False):
    """ This is a step factory that will create a step that tests that all the values in a column
    are unique with respect to each other.  It does not change any values permanently (strip spaces
    or lower-case letters).
    Params
    column: the column class or name of the column in which all values should be unique.
    strip(defaults to True): whether to strip spaces from all values
    ignore_case(defaults to False): whether to lower-case all values
    """
    def safe_strip(value):
        return value.strip() if isinstance(value, str) else value

    column_name = column.name if isinstance(column, Column) else column

    @batch_step
    def check_unique_step(batch, context):
        try:
            values = [row[column_name] for row in batch]
        except KeyError:
            raise DataErrorException(f"Check_unique: Some or all rows did not have '{column_name}' present")
        if strip:
            values = [safe_strip(value) for value in values]
        if ignore_case:
            values = [value.lower() for value in values]
        if len(set(values)) != len(values):
            raise DataErrorException(f"Some values in {column_name} were duplicated, so unique check failed")
        return batch

    return check_unique_step


def sort_by(column):
    """
    This is a step factory that will create a step that orders rows by the values in a give column.
    :param column: The column that will be ordered by when the step is run
    :return: The function that can be added to a phase's list of steps.
    """
    if isinstance(column, Column):
        column_name = column.name
    elif isinstance(column, str):
        column_name = column
    else:
        raise PhaserError("Error declaring sort_by step - expecting column to be a Column or a column name string")

    @batch_step
    def sort_by_step(batch, **kwargs):
        return sorted(batch, key=lambda row: row[column_name])

    return sort_by_step


def filter_rows(func):
    """
    This step factory will drop a bunch of rows. While there are other ways to accomplish the same thing, many of
    those create a DROPPED_ROW message for each one.  This will summarize how many rows were dropped.
    """

    @batch_step(check_size=False)
    def filter_rows_step(batch, context, **kwargs):
        new_batch = [row for row in batch if func(row)]
        num_dropped = len(batch) - len(new_batch)
        if num_dropped > 0:
            context.add_dropped_row(step='filter_rows',
                                    row=None,
                                    message=f"{num_dropped} rows dropped in filter_rows with '{func.__name__}'")
        return new_batch
    return filter_rows_step
