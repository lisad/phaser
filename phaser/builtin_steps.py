from .steps import batch_step, row_step
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
    This step factory will keep only specified rows. While there are other ways to accomplish the same thing, many of
    those create a DROPPED_ROW message for each dropped row.  This will summarize how many rows were dropped.
    Usage:

    filter_rows(lambda row: row['type'] == 'basal')
    filter_rows(lambda row: row['type'] != None)
    filter_rows(lambda row: row['sampleSize'] > 5)  # depends on sampleSize being an IntColumn or cast to int before

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


@row_step
def flatten_all(row, context, **kwargs):
    new_row = row
    still_flattening = True
    while still_flattening:
        # Each iteration will pull nested data up one level. This will repeat until all nested data flattened.
        still_flattening = False
        for key, value in new_row.copy().items():
            # The copy operation is necessary in this loop: you can't add columns to the dict you're looping through
            if isinstance(value, dict):
                still_flattening = True
                new_row, new_columns = _merge_values_if_no_collisions(new_row, key)
    return new_row


def _merge_values_if_no_collisions(row, key_name):
    new_row = row
    new_columns = []
    value = row[key_name]
    for inner_key, inner_value in value.items():
        new_column_name = f"{key_name}__{inner_key}"
        if new_column_name in row.keys():
            raise DataErrorException(f"Error flattening nested data; key {new_column_name} already in row")
        new_row[new_column_name] = inner_value
        new_columns.append(new_column_name)
    del new_row[key_name]
    return new_row, new_columns


def flatten_column(column_name, deep=True):
    """
    The flatten_column step is useful in JSON data handling, which often has value fields within JSON-formatted
    fields within records.  In flattening, new column names are concatenated from hierarchical names.
    For example, flattening a column called 'payload' which is a dict with fields called 'type' and 'id' will create
    one new column for each key called 'payload__type' and 'payload__id'.

    Usage:

    flatten_column('payload')
    flatten_column('performance_detail', deep=False)  # Only flatten one layer -- if there are dicts within dict

    """

    @row_step
    def flatten_col_step(row, context, **kwargs):
        if column_name not in row.keys():
            raise DataErrorException(f"Error flattening nested data - column `{column_name}` not found in row")
        new_row = row.copy()
        if isinstance(row[column_name], dict):
            new_row, new_columns = _merge_values_if_no_collisions(new_row, column_name)
        else:
            # Intentionally doing nothing here - if the value isn't a dict, just keep the value what it is.
            # Example in practice: we could see data with language values that are sometimes strings or dicts, eg
            #                      [{'id': 1, 'title': "Lions and Tigers"},
            #                       {'id': 2, 'title': {'en_US': 'Bears', 'fr_FR': 'Les ours'} }]
            pass

        if deep is True:
            while len(new_columns) > 0:
                column_to_flatten = new_columns.pop()
                value = new_row[column_to_flatten]
                if isinstance(value, dict):
                    new_row, more_new_columns = _merge_values_if_no_collisions(new_row, column_to_flatten)
                    new_columns.extend(more_new_columns)

        return new_row

    return flatten_col_step

