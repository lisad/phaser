import inspect
from collections.abc import Mapping, Sequence
from functools import wraps, partial
import pandas as pd
from .exceptions import DataErrorException, DropRowException, PhaserError
from .pipeline import PHASER_ROW_NUM

ROW_STEP = "ROW_STEP"
BATCH_STEP = "BATCH_STEP"
DATAFRAME_STEP = "DATAFRAME_STEP"
CONTEXT_STEP = "CONTEXT_STEP"
PROBE_VALUE = "__PROBE__"

class StepWrapper():
    def __init__(self, probe, preprocess=None, postprocess=None, handle_exception=None):
        self.probe = probe
        self.preprocess = preprocess
        self.postprocess = postprocess
        self.handle_exception = handle_exception

    def wrap(self, func=None, *, extra_sources=None, extra_outputs=None, **kwargs):
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
            def _step_wrapper(target, context=None, outputs=None, __probe__=None):
                if __probe__ == PROBE_VALUE:
                    return self.probe  # Allows Phase to probe a step for how to call it

                # Note that context will always be passed in during regular operation of phases and pipelines. HOWEVER,
                # we want the DX for tests of wrapped steps to just be simple - call the step with a batch or a row
                # and check the return value.  So instead, we check for context being correct when we need it.
                if context is not None and not "Context" in str(context.__class__):
                    raise PhaserError("A step requiring extra data sources cannot be run without a context")

                try:
                    outputs = outputs or {}
                    kwargs = {}
                    # Special-case a context_step, which we know only takes
                    # the context as the first parameter and thus should not be
                    # defined as a keyword arg as well.
                    if self.probe != CONTEXT_STEP:
                        if 'context' in parameters:
                            kwargs['context'] = context


                    for source in extra_sources:
                        kwargs[source] = context.get_source(source)
                    for out in extra_outputs:
                        if out in outputs:
                            kwargs[out] = outputs[out].data
                        else:
                            raise PhaserError(f"Missing expected output '{out}' in step {step_function.__name__}")

                    if self.preprocess:
                        target = self.preprocess(step_function, target)

                    # We are using a BoundArguments object to make sure apply any
                    # default values to parameters. This is easier than running through
                    # the parameter logic ourselves.
                    bound_args = signature.bind(target, **kwargs)
                    bound_args.apply_defaults()
                    result = step_function(*bound_args.args, **bound_args.kwargs)
                except Exception as exc:
                    if self.handle_exception:
                        self.handle_exception(step_function, exc)
                    else:
                        raise exc


                if self.postprocess:
                    return self.postprocess(step_function, result)
                return result

            return _step_wrapper

        if func is None:
            return _step_argument_wrapper
        else:
            return _step_argument_wrapper(func)

def row_step(func=None, *, extra_sources=None, extra_outputs=None):
    """
    Used to define a step that should run on each row of a data set.

    The function that is decorated should accept a dictionary as its first
    parameter.  If extra_sources or extra_outputs are defined, then they will be
    passed in as explicit parameters, named according to their definition in the
    decorator.  Sources and outputs appear to the step function as the type of
    object defined at the pipeline level.  If a `Context` is required, then it
    can be included as a parameter, too.

    The decorated function must return a dict, or throw an exception.

    :param extra_sources: An array of source names
    :param extra_output: An array of names of outputs
    """

    def postprocess(step_function, result):
        if result is None:
            raise PhaserError("Step should return row.")
        if not isinstance(result, Mapping):
            raise PhaserError(f"Step should return row in dict format, not {result}")
        return result

    wrapper = StepWrapper(ROW_STEP, postprocess=postprocess)
    return wrapper.wrap(func, extra_sources=extra_sources, extra_outputs=extra_outputs)

def batch_step(func=None, *, extra_sources=None, extra_outputs=None, check_size=False):
    """
    Used to define a step that needs to run on the whole batch of data.

    The decorated function should accept a list of dictionaries as its first parameter.

    :param extra_sources: An array of source names
    :param extra_output: An array of names of outputs
    :param check_size: A boolean indicating whether or not to validate the size of the
        batch after the step is run
    """

    def handle_exception(step_function, exc):
        if isinstance(exc, DropRowException):
            raise PhaserError("DropRowException can't be handled in batch steps ") from exc
        raise exc

    def postprocess(step_function, result):
        if not isinstance(result, Sequence):
            raise PhaserError(
                f"Step {step_function} returned a {result.__class__} rather than a list of rows")
        return result, check_size

    wrapper = StepWrapper(BATCH_STEP, postprocess=postprocess, handle_exception=handle_exception)
    return wrapper.wrap(func, extra_sources=extra_sources, extra_outputs=extra_outputs)

def dataframe_step(func=None, *, pass_row_nums=True, extra_sources=None, extra_outputs=None, check_size=False):
    """
    Used to define a step that needs to run on the whole set of data as a `pandas.DataFrame`.

    The decorated function should accept a DataFrame as its first parameter.

    :param pass_row_nums: If True, the row numbers will be set in the DataFrame
        in a column named the value of `PHASER_ROW_NUM`
    :param extra_sources: An array of source names
    :param extra_output: An array of names of outputs
    :param check_size: A boolean indicating whether or not to validate the size of the
        DataFrame after the step is run
    """
    def handle_exception(step_function, exc):
        if isinstance(exc, DropRowException):
            raise PhaserError("DropRowException can't be handled in steps operating on bulk data ") from exc
        raise exc

    def preprocess(step_function, target):
        dataframe = pd.DataFrame.from_records(target)
        if pass_row_nums:
            dataframe[PHASER_ROW_NUM] = [row.row_num for row in target]
        return dataframe

    def postprocess(step_function, result):
        if not isinstance(result, pd.DataFrame):
            raise PhaserError(
                f"Step {step_function} returned a {result.__class__} rather than a pandas DataFrame")
        return result.to_dict(orient='records'), check_size

    wrapper = StepWrapper(
        DATAFRAME_STEP,
        preprocess=preprocess,
        postprocess=postprocess,
        handle_exception=handle_exception,
    )
    return wrapper.wrap(func, extra_sources=extra_sources, extra_outputs=extra_outputs)


def context_step(func=None, *, extra_sources=None, extra_outputs=None):
    """
    Used to define a step that works on the context.

    The decorated function should accept the context as its first parameter.

    :param extra_sources: An array of source names
    :param extra_output: An array of names of outputs
    """
    def postprocess(step_function, result):
        if result is not None:
            raise PhaserError(f"Context steps are not expected to return a value (step is {step_function})")

    wrapper = StepWrapper(CONTEXT_STEP, postprocess=postprocess)
    return wrapper.wrap(func, extra_sources=extra_sources, extra_outputs=extra_outputs)
