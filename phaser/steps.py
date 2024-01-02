from functools import wraps


def row_step(step_function):
    """ This decorator is used to indicate a step that should run on each row of a data set.
    It adds a "probe" response to the step that allows the phase running logic to know this.
    """
    @wraps(step_function)
    def _row_step_wrapper(phase, row):
        if row == "__PROBE__":
            return "row_step"  # Allows Phase to probe a step for how to call it
        result = step_function(phase, row)
        assert isinstance(result, dict)
        return result
    return _row_step_wrapper


def batch_step(step_function):
    """ This decorator allows the Phase run logic to determine that this step needs to run on the
    whole batch by adding a 'probe' response
    """
    @wraps(step_function)
    def _batch_step_wrapper(phase, batch):
        if batch == "__PROBE__":
            return "batch_step"
        result = step_function(phase, batch)
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
    def check_unique_step(phase, batch):
        values = [row.get(column_name) for row in batch]
        if strip:
            values = [value.strip() for value in values]
        if ignore_case:
            values = [value.lower() for value in values]
        assert len(set(values)) == len(values)
        return batch

    return check_unique_step
