from functools import wraps
from .pipeline import PipelineErrorException, PhaserException
from .column import Column

ROW_STEP = "ROW_STEP"
BATCH_STEP = "BATCH_STEP"
DATAFRAME_STEP = "DATAFRAME_STEP"
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
            raise PipelineErrorException("Step should return row.")
        if not isinstance(result, dict):
            raise PipelineErrorException(f"Step should return row in dict format, not {result}")
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
        result = step_function(batch, context=context)
        if not isinstance(result, list):
            raise PipelineErrorException(
                f"Step {step_function} returned a {result.__class__} rather than a list of rows")
        return result
    return _batch_step_wrapper


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
            raise PipelineErrorException(f"Check_unique: Some or all rows did not have '{column_name}' present")
        if strip:
            values = [safe_strip(value) for value in values]
        if ignore_case:
            values = [value.lower() for value in values]
        if len(set(values)) != len(values):
            raise PipelineErrorException(f"Some values in {column_name} were duplicated, so unique check failed")
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
        raise PhaserException("Error declaring sort_by step - expecting column to be a Column or a column name string")

    @batch_step
    def sort_by_step(batch, **kwargs):
        return sorted(batch, key=lambda row: row[column_name])

    return sort_by_step
