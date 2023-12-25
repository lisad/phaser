
def row_step(step_function):
    """ This decorator is used to indicate a step that should run on each row of a data set.

    """
    def _row_step_wrapper(phase, row):
        if row == "__PROBE__":
            return "row_step"  # Allows Phase to probe a step for how to call it
        result = step_function(phase, row)
        assert isinstance(result, dict)
        return result
    return _row_step_wrapper