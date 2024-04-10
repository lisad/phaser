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


def row_step(func=None, *, extra_sources=None, extra_outputs=None):
    """ This decorator is used to indicate a step that should run on each row of a data set.
    It adds a "probe" response to the step that allows the phase running logic to know this.
    """

    # Initialize extra_sources and extra_outputs to a default new list if none
    # was passed in. Do not use default parameters, since the default value is
    # evaluated only once and would therefore use the same underlying mutable
    # list for subsequent calls to the function.
    # Reference: https://docs.python.org/3/tutorial/controlflow.html#default-argument-values
    extra_sources = extra_sources or []
    extra_outputs = extra_outputs or []

    def _step_argument_wrapper(step_function):
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
        def _row_step_wrapper(row, context=None, outputs=None, __probe__=None):
            if __probe__ == PROBE_VALUE:
                return ROW_STEP  # Allows Phase to probe a step for how to call it

            outputs = outputs or {}
            kwargs = {}
            if 'context' in parameters:
                kwargs['context'] = context
            for source in extra_sources:
                kwargs[source] = context.get_source(source)
            for out in extra_outputs:
                if out in outputs:
                    kwargs[out] = outputs[out].data
                else:
                    raise PhaserError(f"Missing expected output '{out}' in step {step_function.__name__}")

            # We are using a BoundArguments object to make sure apply any
            # default values to parameters. This is easier than running through
            # the parameter logic ourselves.
            bound_args = signature.bind(row, **kwargs)
            bound_args.apply_defaults()
            result = step_function(*bound_args.args, **bound_args.kwargs)
            if result is None:
                raise PhaserError("Step should return row.")
            if not isinstance(result, Mapping):
                raise PhaserError(f"Step should return row in dict format, not {result}")
            return result
        return _row_step_wrapper

    if func is None:
        return _step_argument_wrapper
    else:
        return _step_argument_wrapper(func)


def batch_step(func=None, *, extra_sources=None, extra_outputs=None):
    """ This decorator allows the Phase run logic to determine that this step needs to run on the
    whole batch by adding a 'probe' response
    """

    # Initialize extra_sources and extra_outputs to a default new list if none
    # was passed in. Do not use default parameters, since the default value is
    # evaluated only once and would therefore use the same underlying mutable
    # list for subsequent calls to the function.
    # Reference: https://docs.python.org/3/tutorial/controlflow.html#default-argument-values
    extra_sources = extra_sources or []
    extra_outputs = extra_outputs or []

    def _step_argument_wrapper(step_function):
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
        def _batch_step_wrapper(batch, context=None, outputs=None, __probe__=None):
            if __probe__ == PROBE_VALUE:
                return BATCH_STEP
            try:
                outputs = outputs or {}
                kwargs = {}
                if 'context' in parameters:
                    kwargs['context'] = context
                for source in extra_sources:
                    kwargs[source] = context.get_source(source)
                for out in extra_outputs:
                    if out in outputs:
                        kwargs[out] = outputs[out].data
                    else:
                        raise PhaserError(f"Missing expected output '{out}' in step {step_function.__name__}")

                # We are using a BoundArguments object to make sure apply any
                # default values to parameters. This is easier than running through
                # the parameter logic ourselves.
                bound_args = signature.bind(batch, **kwargs)
                bound_args.apply_defaults()
                result = step_function(*bound_args.args, **bound_args.kwargs)
            except DropRowException as exc:
                raise PhaserError("DropRowException can't be handled in batch steps ") from exc
            if not isinstance(result, Sequence):
                raise PhaserError(
                    f"Step {step_function} returned a {result.__class__} rather than a list of rows")
            return result
        return _batch_step_wrapper

    if func is None:
        return _step_argument_wrapper
    else:
        return _step_argument_wrapper(func)

def dataframe_step(func=None, *, pass_row_nums=True, extra_sources=None, extra_outputs=None):
    # Initialize extra_sources and extra_outputs to a default new list if none
    # was passed in. Do not use default parameters, since the default value is
    # evaluated only once and would therefore use the same underlying mutable
    # list for subsequent calls to the function.
    # Reference: https://docs.python.org/3/tutorial/controlflow.html#default-argument-values
    extra_sources = extra_sources or []
    extra_outputs = extra_outputs or []

    def _step_argument_wrapper(step_function):
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
        def _df_step_wrapper(row_data, context=None, outputs=None, __probe__=None):
            if __probe__ == PROBE_VALUE:
                return DATAFRAME_STEP
            try:
                dataframe = pd.DataFrame.from_records(row_data)
                if pass_row_nums:
                    dataframe[PHASER_ROW_NUM] = [row.row_num for row in row_data]

                outputs = outputs or {}
                kwargs = {}
                if 'context' in parameters:
                    kwargs['context'] = context
                for source in extra_sources:
                    kwargs[source] = context.get_source(source)
                for out in extra_outputs:
                    if out in outputs:
                        kwargs[out] = outputs[out].data
                    else:
                        raise PhaserError(f"Missing expected output '{out}' in step {step_function.__name__}")

                # We are using a BoundArguments object to make sure apply any
                # default values to parameters. This is easier than running through
                # the parameter logic ourselves.
                bound_args = signature.bind(dataframe, **kwargs)
                bound_args.apply_defaults()
                result = step_function(*bound_args.args, **bound_args.kwargs)
            except DropRowException as exc:
                raise PhaserError("DropRowException can't be handled in steps operating on bulk data ") from exc
            if not isinstance(result, pd.DataFrame):
                raise PhaserError(
                    f"Step {step_function} returned a {result.__class__} rather than a pandas DataFrame")
            return result.to_dict(orient='records')
        return _df_step_wrapper

    if func is None:
        return _step_argument_wrapper
    else:
        return _step_argument_wrapper(func)

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
