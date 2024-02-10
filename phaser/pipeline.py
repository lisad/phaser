import inspect
import logging
import os
from pathlib import PosixPath

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


class PhaserException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class PipelineErrorException(PhaserException):
    """ Using this exception will cause the error to stop the pipeline, and this will be reported as an error.
    If possible, the pipeline will keep going and collect more errors until it reaches the end of a phase.  """
    pass


class DropRowException(PhaserException):
    """ Throwing this exception in a row_step will cause the current row to be dropped. """
    pass


class WarningException(PhaserException):
    """ Throwing this exception will add warnings to the output of the pipeline.  While it can't be used
    in methods where a return value is needed, it can be used in methods that check results without returning
    fixed data.  """
    pass


def _stringify_step(step):
    if isinstance(step, str):
        return step
    try:
        return getattr(step, '__name__')
    except Exception as e:
        logger.error(f"Unknown case trying to turn {step} into a step name")
        raise e


class Context:
    def __init__(self, variables=None):
        self.errors = {}
        self.warnings = {}
        self.variables = variables or {}
        self.current_row = None
        self.dropped_rows = {}

    def add_error(self, step, row, message):
        # LMDTODO Am I passing row or getting row from context?
        step_name = _stringify_step(step)
        if self.current_row is None:
            raise Exception("Code error: Pipeline Context should always know what row we're operating on")
        self.errors[self.current_row] = {'step': step_name, 'message': message, 'row': row}

    def add_warning(self, step, row, message):
        step_name = _stringify_step(step)
        if self.current_row is None:
            raise Exception("Code error: Pipeline Context should always know what row we're operating on")

        warning_data = {'step': step_name, 'message': message, 'row': row}
        if self.current_row in self.warnings.keys():
            self.warnings[self.current_row].append(warning_data)
        else:
            self.warnings[self.current_row] = [warning_data]

    def add_dropped_row(self, step, row, message):
        step_name = _stringify_step(step)
        if self.current_row is None:
            raise Exception("Code error: Pipeline Context should always know what row we're operating on")
        self.dropped_rows[self.current_row] = {'step': step_name, 'message': message, 'row': row}

    def add_variable(self, name, value):
        """ Add variables that are global to the pipeline and accessible to steps and internal methods """
        self.variables[name] = value

    def get(self, name):
        return self.variables.get(name)

    def has_errors(self):
        return self.errors != {}


class Pipeline:
    # Subclasses can override here to set values for all instances, or override in instantiation
    working_dir = None
    source = None
    phases = []

    ROW_NUM_FIELD = "__phaser_row_num__"

    ON_ERROR_WARN = "ON_ERROR_WARN"
    ON_ERROR_COLLECT = "ON_ERROR_COLLECT"
    ON_ERROR_DROP_ROW = "ON_ERROR_DROP_ROW"
    ON_ERROR_STOP_NOW = "ON_ERROR_STOP_NOW"

    def __init__(self, working_dir=None, source=None, phases=None, diff=False):
        self.working_dir = working_dir or self.__class__.working_dir
        if self.working_dir and not os.path.exists(self.working_dir):
            raise ValueError(f"Working dir {self.working_dir} does not exist.")
        self.source = source or self.__class__.source
        assert self.source is not None and self.working_dir is not None
        self.phases = phases or self.__class__.phases
        self.phase_instances = []
        self.diff = diff

    def setup_phases(self):
        """ Instantiates phases passed as classes, and assigns unique names to phases"""
        phase_names = []
        for phase in self.phases:
            phase_instance = phase
            if inspect.isclass(phase):
                phase_instance = phase()
            if self.diff:
                # Only import DiffingPhase if we need it. This helps to prevent
                # circular dependencies of imports during initialization.
                from .differ import DiffingPhase
                phase_instance = DiffingPhase(phase_instance)
            name = phase_instance.name
            i = 1
            while name in phase_names:
                name = f"{phase.name}-{i}"
                i = i + 1
            phase_instance.name = name
            self.phase_instances.append(phase_instance)

    def run(self):
        self.setup_phases()
        if self.source is None:
            raise ValueError("Pipeline source may not be None")
        next_source = self.source
        for phase in self.phase_instances:
            destination = self.get_destination(phase)
            phase.run(source=next_source, destination=destination)
            next_source = destination  # for next phase in chain

    def get_destination(self, phase):
        source_filename = None
        if isinstance(self.source, str):
            source_filename = os.path.basename(self.source)
        elif isinstance(self.source, PosixPath):
            source_filename = self.source.name
        else:
            raise ValueError(f"Pipeline 'source' is not a string or Path ({self.source.__class__}")

        dest_filename = f"{phase.name}_output_{source_filename}"

        destination = os.path.join(self.working_dir, dest_filename)
        if str(destination) == str(self.source):
            raise ValueError("Destination file cannot be same as source, it will overwrite")
        return destination
