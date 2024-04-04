import inspect
from collections.abc import Mapping, Sequence
from functools import wraps, partial
import pandas as pd
from .exceptions import DataErrorException, DropRowException, PhaserError
from .pipeline import PHASER_ROW_NUM
from .column import Column
from .records import Records

ROW_STEP = "ROW_STEP"
BATCH_STEP = "BATCH_STEP"
DATAFRAME_STEP = "DATAFRAME_STEP"
CONTEXT_STEP = "CONTEXT_STEP"
PROBE_VALUE = "__PROBE__"


def row_step(func=None, *, extra_sources=[], extra_outputs=[]):
    """ This decorator is used to indicate a step that should run on each row of a data set.
    It adds a "probe" response to the step that allows the phase running logic to know this.
    """
    def _row_step_argument_wrapper(step_function):
        signature = inspect.signature(step_function)
        parameters = signature.parameters
        # Check that the step_function signature matches what is expected if
        # extra_sources or extra_outputs have been specified.
        if extra_sources or extra_outputs:
            missing_sources = [
                source
                for source in extra_sources
                if source not in parameters
            ]
            missing_outputs = [
                output
                for output in extra_outputs
                if output not in parameters
            ]
            if missing_sources or missing_outputs:
                missing_sources.extend(missing_outputs)
                raise PhaserError(f"{step_function.__name__}() missing parameter: {', '.join(map(str, missing_sources))}")


        @wraps(step_function)
        def _row_step_wrapper(row, context=None, outputs={}, __probe__=None):
            if __probe__ == PROBE_VALUE:
                return ROW_STEP  # Allows Phase to probe a step for how to call it
            print(f"ROW STEP WRAPPER {outputs=} {id(outputs)}")
            kwargs = {}
            if 'context' in parameters:
                kwargs['context'] = context
            for source in (extra_sources or []):
                kwargs[source] = context.get_source(source)
            for out in extra_outputs:
                if out in outputs:
                    kwargs[out] = outputs[out]
                else:
                    # TODO: Raise exception if phase did not pass in an output
                    pass

            print(f"calling {step_function} with {row=} and {kwargs=}")
            # TODO: Figure out how to apply any default values, or use an
            # inspect.BoundArguments.apply_defaults to call the function.
            result = step_function(row, **kwargs)
            # result = step_function(row, context=context)
            if result is None:
                raise PhaserError("Step should return row.")
            if not isinstance(result, Mapping):
                raise PhaserError(f"Step should return row in dict format, not {result}")
            return result
        return _row_step_wrapper

    if func is None:
        return _row_step_argument_wrapper
    else:
        return _row_step_argument_wrapper(func)


def batch_step(step_function):
    """ This decorator allows the Phase run logic to determine that this step needs to run on the
    whole batch by adding a 'probe' response
    """
    @wraps(step_function)
    def _batch_step_wrapper(batch, context=None, __probe__=None):
        if __probe__ == PROBE_VALUE:
            return BATCH_STEP
        try:
            # LMDTODO: Discovered in testing that if the step function doesn't declare the context in its params,
            # when the calling code tries to pass it a batch AND a context, it doesn't even run and its hard to see why.
            # See test test_batch_step_missing_param. The TODO is to fix this for the other step types as well.
            if 'context' in inspect.signature(step_function).parameters.keys():
                result = step_function(batch, context=context)
            else:
                result = step_function(batch)
        except DropRowException as exc:
            raise PhaserError("DropRowException can't be handled in batch steps ") from exc
        if not isinstance(result, Sequence):
            raise PhaserError(
                f"Step {step_function} returned a {result.__class__} rather than a list of rows")
        return result
    return _batch_step_wrapper


def dataframe_step(step_function=None, pass_row_nums=True):
    if step_function is None:
        return partial(dataframe_step, pass_row_nums=pass_row_nums)

    @wraps(step_function)
    def _df_step_wrapper(row_data, context=None, __probe__=None):
        if __probe__ == PROBE_VALUE:
            return DATAFRAME_STEP
        try:
            dataframe = pd.DataFrame.from_records(row_data)
            if pass_row_nums:
                dataframe[PHASER_ROW_NUM] = [row.row_num for row in row_data]
            if 'context' in str(inspect.signature(step_function)):
                result = step_function(dataframe, context=context)
            else:
                result = step_function(dataframe)
        except DropRowException as exc:
            raise PhaserError("DropRowException can't be handled in steps operating on bulk data ") from exc
        if not isinstance(result, pd.DataFrame):
            raise PhaserError(
                f"Step {step_function} returned a {result.__class__} rather than a pandas DataFrame")
        return result.to_dict(orient='records')
    return _df_step_wrapper


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
