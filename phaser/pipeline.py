import inspect
import logging
import os
from pathlib import PosixPath
from .io import read_csv, save_csv
from .exceptions import *
from .records import Records, Record

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


PHASER_ROW_NUM = '__phaser_row_num__'


def _stringify_step(step):
    if isinstance(step, str):
        return step
    try:
        return getattr(step, '__name__')
    except Exception as e:
        logger.error(f"Unknown case trying to turn {step} into a step name")
        raise e


def _extract_row_num(row):
    if not row:
        return 'none'
    if isinstance(row, Record):
        return row.row_num
    return row.get(PHASER_ROW_NUM, 'unknown')


class Context:
    """ Context is created by the pipeline, and passed to each phase.  Thus, it can be used
    to carry extra data or variable values between phases if necessary. """

    def __init__(self, variables=None, working_dir=None):
        self.reset_events()
        self.variables = variables or {}
        self.current_row = None
        # Stores sources and outputs as ReadWriteObjects
        self.rwos = {}
        self.working_dir = working_dir

    def reset_events(self):
        self.errors = {}
        self.warnings = {}
        self.dropped_rows = {}

    def add_error(self, step, row, message):
        step_name = _stringify_step(step)
        index = _extract_row_num(row)
        if index in self.errors:
            raise PhaserError(f"Reporting 2 errors on same row ({index}) not handled yet")
            # LMDTODO This only captures one error per row number or one error for 'unknown'. that seems fine for
            # now because we stop processing errored rows, but eventually can be more complete
        else:
            self.errors[index] = {'step': step_name, 'message': message, 'row': row}

    def add_warning(self, step, row, message):
        step_name = _stringify_step(step)
        index = _extract_row_num(row)
        warning_data = {'step': step_name, 'message': message, 'row': row}
        # LMDTODO to simplify this, self.warnings can be a defaultdict with default to array
        if index in self.warnings:
            self.warnings[index].append(warning_data)
        else:
            self.warnings[index] = [warning_data]

    def add_dropped_row(self, step, row, message):
        step_name = _stringify_step(step)
        index = _extract_row_num(row)
        # LMDTODO: we should think about preventing rows from being dropped twice, although
        # if rows are renumbered starting from 1, the same row num could be dropped twice.  we're collecting
        # more arguments towards robust, unique row numbers using generations or a sequence that does not
        # restart from 1.
        if index in self. dropped_rows:
            raise PhaserError(f"Dropping same row ({index}) twice not handled properly yet")
        else:
            self.dropped_rows[index] = {'step': step_name, 'message': message, 'row': row}

    def add_variable(self, name, value):
        """ Add variables that are global to the pipeline and accessible to steps and internal methods """
        self.variables[name] = value

    def get(self, name):
        return self.variables.get(name)

    def has_errors(self):
        return self.errors != {}

    def set_output(self, name, data):
        # At present outputs must be in record format and save to CSV, but this should be expanded.
        if name in self.rwos:
            logger.warning("Overwriting while adding output '%s'", name)
        self.rwos[name] = ReadWriteObject(name, data, to_save=True)

    def set_source(self, name, data):
        if name in self.rwos:
            logger.warning("Overwriting while setting source '%s'", name)
        self.rwos[name] = ReadWriteObject(name, data, to_save=False)

    def get_source(self, name):
        if name in self.rwos:
            return self.rwos[name].data
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
            return phase.extra_sources
        return []

    def _find_extra_outputs(phase):
        if hasattr(phase, 'extra_outputs'):
            return phase.extra_outputs
        return []

    def init_source(self, name, source):
        """ Initializes a named source based on the kind of 'source' passed in.

        :param source: must be a os.PathLike file in csv format and will be read entirely into memory
        """
        # TODO: Check that file exists first?
        data = read_csv(source)
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
            if not source in self.context.rwos:
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
        data = Records(self.load(source))
        phase.load_data(data)
        results = phase.run()
        self.save(results.for_save(), destination)
        self.check_extra_outputs(phase)
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
            print(f"DROP ROW in step {info['step']}, row {row_num}: message: '{info['message']}'")
        # Unlike errors and dropped rows, there can be multiple warnings per row
        for row_num, warnings in self.context.warnings.items():
            for warning in warnings:
                print(f"WARNING in step {warning['step']}, row {row_num}, message: '{warning['message']}'")
        for row_num, error in self.context.errors.items():
            print(f"ERROR in step {error['step']}, row {row_num}, message: '{error['message']}'")

    def check_extra_outputs(self, phase):
        """ Check that any extra outputs the phase declared have been added into the context.
        Throws a PhaserError if any do not exist, as that is a programming error that should be fixed."""
        missing = []
        for name in Pipeline._find_extra_outputs(phase):
            if name not in self.context.rwos:
                missing.append(name)
        if len(missing) > 0:
            raise PhaserError(f"Phase {phase.name} missing extra_outputs: {missing}")

    def save_extra_outputs(self):
        for item in self.context.rwos.values():
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
