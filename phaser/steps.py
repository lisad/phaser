from collections.abc import Mapping, Sequence
from functools import wraps
from .pipeline import DataErrorException, DataException, DropRowException, PhaserError
from .column import Column

ROW_STEP = "ROW_STEP"
BATCH_STEP = "BATCH_STEP"
CONTEXT_STEP = "CONTEXT_STEP"
PROBE_VALUE = "__PROBE__"

def row_step(step_function):
    """ This decorator is used to indicate a step that should run on each row of a data set.
    It adds a "probe" response to the step that allows the phase running logic to know this.
    """
    @wraps(step_function)
    def _row_step_wrapper(row, context=None, __probe__=None):
        if __probe__ == PROBE_VALUE:
            return ROW_STEP  # Allows Phase to probe a step for how to call it
        result = step_function(row, context=context)
        if result is None:
            raise PhaserError("Step should return row.")
        if not isinstance(result, Mapping):
            raise PhaserError(f"Step should return row in dict format, not {result}")
        return result
    return _row_step_wrapper


def batch_step(step_function):
    """ This decorator allows the Phase run logic to determine that this step needs to run on the
    whole batch by adding a 'probe' response
    """
    @wraps(step_function)
    def _batch_step_wrapper(batch, context=None, __probe__=None):
        if __probe__ == PROBE_VALUE:
            return BATCH_STEP
        try:
            result = step_function(batch, context=context)
        except DropRowException as exc:
            raise PhaserError("DropRowException can't be handled in batch steps ") from exc
        if not isinstance(result, Sequence):
            raise PhaserError(
                f"Step {step_function} returned a {result.__class__} rather than a list of rows")
        return result
    return _batch_step_wrapper


def context_step(step_function):
    @wraps(step_function)
    def _context_step_wrapper(context, __probe__=None):
        if __probe__ == PROBE_VALUE:
            return CONTEXT_STEP
        result = step_function(context)
        if result is not None:
            raise PhaserError(f"Context steps are not expected to return a value (step is {step_function})")

    return _context_step_wrapper


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
