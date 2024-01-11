from functools import wraps

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
        assert isinstance(result, dict)
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
        assert isinstance(result, list)
        return result
    return _batch_step_wrapper


def check_unique(column_name, strip=True, ignore_case=False):
    """ This is a step factory that will create a step that tests that all the values in a column
    are unique with respect to each other.  It does not change any values permanently (strip spaces
    or lower-case letters).
    Params
    column_name: the name of the column in which all values should be unique.
    strip(defaults to True): whether to strip spaces from all values
    ignore_case(defaults to False): whether to lower-case all values
    """

    @batch_step
    def check_unique_step(batch, **kwargs):
        values = [row.get(column_name) for row in batch]
        if strip:
            values = [value.strip() for value in values]
        if ignore_case:
            values = [value.lower() for value in values]
        assert len(set(values)) == len(values)
        return batch

    return check_unique_step
