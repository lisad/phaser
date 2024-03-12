import inspect
import logging
import os
from phaser.io import read_csv, save_csv
from pathlib import PosixPath
from phaser.exceptions import *

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


def _stringify_step(step):
    if isinstance(step, str):
        return step
    try:
        return getattr(step, '__name__')
    except Exception as e:
        logger.error(f"Unknown case trying to turn {step} into a step name")
        raise e


class Context:
    """ Context is created by the pipeline, and passed to each phase.  Thus, it can be used
    to carry extra data or variable values between phases if necessary. """

    def __init__(self, variables=None, working_dir=None):
        self.reset_events()
        self.variables = variables or {}
        self.current_row = None
        self.sources = {}
        self.outputs = []
        self.working_dir = working_dir

    def reset_events(self):
        self.errors = {}
        self.warnings = {}
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

    def add_output(self, name, data):
        # At present outputs must be in record format and save to CSV, but this should be expanded.
        self.outputs.append(ReadWriteObject(name, data, to_save=True))

    def set_source(self, name, data):
        self.sources[name] = data

    def get_source(self, name):
        if name in self.sources.keys():
            return self.sources[name]
        raise PhaserError(f"Source not loaded before being used: {name}")


class ReadWriteObject:
    def __init__(self, name, data=None, to_save=True):
        self.name = name
        self.format = 'csv'
        self.data = data
        self.to_save = to_save


class Pipeline:
    """ Pipeline handles running phases in order.  It also handles I/O and marshalling what
    outputs from phases get used as inputs in later phases.  """
    working_dir = None
    source = None
    phases = []

    ON_ERROR_WARN = "ON_ERROR_WARN"
    ON_ERROR_COLLECT = "ON_ERROR_COLLECT"
    ON_ERROR_DROP_ROW = "ON_ERROR_DROP_ROW"
    ON_ERROR_STOP_NOW = "ON_ERROR_STOP_NOW"

    def __init__(self, working_dir=None, source=None, phases=None):
        self.working_dir = working_dir or self.__class__.working_dir
        if self.working_dir and not os.path.exists(self.working_dir):
            raise ValueError(f"Working dir {self.working_dir} does not exist.")
        self.source = source or self.__class__.source
        assert self.source is not None and self.working_dir is not None
        self.phases = phases or self.__class__.phases
        self.phase_instances = []
        self.context = Context(working_dir=self.working_dir)
        # Collect the extra sources and outputs from the phases so they can be
        # reconciled and initialized as necessary.  Extra sources that match
        # with extra outputs do not need to be initialized, but extra sources
        # that do not come from phases must be initialized so that phases can
        # access their data.
        self.extra_sources = [
            source for phase in self.phases for source in Pipeline._find_extra_sources(phase)
        ]
        self.extra_outputs = [
            output for phase in self.phases for output in Pipeline._find_extra_outputs(phase)
        ]
        self.sources_needing_initialization = [
            source
            for source in self.extra_sources
            if source not in self.extra_outputs
        ]

    def _find_extra_sources(phase):
        if hasattr(phase, 'extra_sources'):
            print(f"Phase {phase} has extra_sources: {phase.extra_sources}")
            return phase.extra_sources
        return []

    def _find_extra_outputs(phase):
        if hasattr(phase, 'extra_outputs'):
            print(f"Phase {phase} has extra_outputs: {phase.extra_outputs}")
            return phase.extra_outputs
        return []

    def init_source(self, name, source):
        """ Initializes a named source based on the kind of 'source' passed in.

        :param source: must be a os.PathLike file in csv format and will be read entirely into memory
        """
        # TODO: Check that file exists first?
        data = read_csv(source).to_dict('records')
        self.context.set_source(name, data)

    def setup_phases(self):
        """ Instantiates phases passed as classes, assigns unique names to phases, and passes
         Context in also. """
        phase_names = []
        for phase in self.phases:
            phase_instance = phase
            if inspect.isclass(phase):
                phase_instance = phase(context=self.context)
            else:
                phase.context = self.context
            name = phase_instance.name
            i = 1
            while name in phase_names:
                name = f"{phase.name}-{i}"
                i = i + 1
            phase_instance.name = name
            self.phase_instances.append(phase_instance)

    def validate_sources(self):
        """ Check that all required sources have been initialized."""
        missing_sources = []
        for source in self.sources_needing_initialization:
            if not source in self.context.sources:
                missing_sources.append(source)

        if len(missing_sources) > 0:
            raise PhaserError(f"{len(missing_sources)} sources need initialization: {missing_sources}")

    def run(self):
        self.setup_phases()
        self.validate_sources()
        if self.source is None:
            raise ValueError("Pipeline source may not be None")
        next_source = self.source
        for phase in self.phase_instances:
            destination = self.get_destination(phase)
            self.run_phase(phase, next_source, destination)
            next_source = destination

    def run_phase(self, phase, source, destination):
        logger.info(f"Loading input from {source} for {phase.name}")
        data = self.load(source)
        phase.load_data(data)
        results = phase.run()
        self.save(results, destination)
        self.save_extra_outputs()
        logger.info(f"{phase.name} saved output to {destination}")
        self.report_errors_and_warnings(phase.name)
        if self.context.has_errors():
            raise DataException(f"Phase '{phase.name}' failed with {len(self.context.errors.keys())} errors.")
        else:
            self.context.reset_events()

    def report_errors_and_warnings(self, phase_name):
        """ TODO: different formats, flexibility
        For CLI operation we want to report errors to the CLI, but for unsupervised operation these should go
        to logs.  Python logging does allow users of a library to send log messages to more than one place while
        customizing log level desired, and we could have drop-row messages as info and warning as warn level so
        these fit very nicely into the standard levels allowing familiar customization.  """
        print(f"Reporting for phase {phase_name}")
        for row_num, info in self.context.dropped_rows.items():
            print(f"DROPPED row: {row_num}, message: '{info['message']}'")
        # Unlike errors and dropped rows, there can be multiple warnings per row
        for row_num, warnings in self.context.warnings.items():
            for warning in warnings:
                print(f"WARNING row: {row_num}, message: '{warning['message']}'")
        for row_num, error in self.context.errors.items():
            print(f"ERROR row: {row_num}, message: '{error['message']}'")

    def save_extra_outputs(self):
        for item in self.context.outputs:
            # Since context is passed from Phase to Phase, only save the new ones with to_save=True
            if item.to_save:
                filename = self.working_dir / f"{item.name}.csv"
                if os.path.exists(filename):
                    raise PhaserError(f"Output with name '{filename}' exists.  Aborting before overwrite.")
                save_csv(filename, item.data)
                logger.info(f"Extra output {item.name} saved to {self.working_dir}")
                item.to_save = False

    def load(self, source):
        """ The load method can be overridden to apply a pipeline-specific way of loading data.
        Phaser default is to read data from a CSV file. """
        return read_csv(source)

    def save(self, results, destination):
        """ This method saves the result of the Phase operating on the batch, in phaser's preferred format.
        It should be easy to override this method to save in a different way, using pandas' to_csv, to_excel, to_json
        or a different output entirely.

        CSV defaults chosen:
        * separator character is ','
        * encoding is UTF-8
        * compression will be attempted if filename ends in 'zip', 'gzip', 'tar' etc
        """

        # Use the raw list(dict) form of the data, because DataFrame
        # construction does something different with a subclass of Sequence and
        # Mapping that results in the columns being re-ordered.
        save_csv(destination, results)

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
