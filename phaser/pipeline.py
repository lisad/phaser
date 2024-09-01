import inspect
import logging
import os
from datetime import datetime
from pathlib import Path, PosixPath

from .context import Context
from .io import read_csv, save_csv, IOObject
from .exceptions import *
from .records import Records
from .constants import *


logger = logging.getLogger(__name__)

class Pipeline:
    """ Pipeline handles running phases in order.  It also handles I/O and marshalling what
    outputs from phases get used as inputs in later phases.  """
    working_dir = None
    source = None
    phases = []

    def __init__(self, working_dir=None, source=None, phases=None, verbose=False, error_policy=None, name="pipeline"):
        self.working_dir = working_dir or self.__class__.working_dir
        if self.working_dir and not os.path.exists(self.working_dir):
            raise ValueError(f"Working dir {self.working_dir} does not exist.")
        self.source = source or self.__class__.source
        assert self.source is not None and self.working_dir is not None

        timestamp = datetime.today().strftime("%y%m%d-%H%M%S")
        self.prev_run_dir = Path(self.working_dir / f"{name}-{timestamp}")
        self.errors_and_warnings_file = self.working_dir / "errors_and_warnings.txt"

        self.phases = phases or self.__class__.phases
        try:
            iter(self.phases)
        except TypeError:
            self.phases = [self.phases]
        self.phase_instances = []
        self.verbose = verbose
        self.context = Context(working_dir=self.working_dir, verbose=self.verbose, error_policy=error_policy)

        self.setup_phases()
        self.setup_extras()

    def init_source(self, name, source_path):
        """ Initializes a named source based on the kind of 'source' passed in.

        :param name: An IOObject that specifies the name and type of the source
        :param source_path: must be a os.PathLike file in csv format and will be read entirely into memory
        """
        source = next((s for s in self.extra_sources if isinstance(s, IOObject) and s.name == name), None)
        if not source:
            raise PhaserError(f"Unable to find source {name} to initialize")

        source.load(source_path)
        self.context.set_source(name, source)

    def setup_phases(self):
        """ Instantiates phases passed as classes, assigns unique names to phases, and passes
         Context in also. """

        # Prevent us from re-instantiating the phases if they have been set up
        # already.
        if self.phase_instances:
            return

        phase_names = []
        for phase in self.phases:
            try:
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
            except Exception as exc:
                raise PhaserError(f"Error setting up {phase} instance") from exc


    def setup_extras(self):
        # Phases must be instantiated, because that is how any configuration set
        # on the class of the phase is set on the instance. Otherwise, we would
        # need to check for sources and outputs on the classes and the
        # instancees.
        self.extra_sources = [
            source for phase in self.phase_instances for source in phase.extra_sources
        ]

    def sources_needing_initialization(self):
        # Collect the extra sources and outputs from the phases so they can be
        # reconciled and initialized as necessary.  Extra sources that match
        # with extra outputs do not need to be initialized, but extra sources
        # that do not come from phases must be initialized so that phases can
        # access their data.
        # TODO: This logic should check that expected outputs come from sources
        # from prior phases. As written an output could be specified in a phase
        # that will run after a phase that needs its source.
        extra_output_names = [
            output.name for phase in self.phase_instances for output in phase.extra_outputs
        ]
        return [
            (source if isinstance(source, str) else source.name)
            for source in self.extra_sources
            if (source if isinstance(source, str) else source.name) not in extra_output_names
        ]

    def validate_sources(self):
        """ Check that all required sources have been initialized."""
        missing_sources = []
        for source in self.sources_needing_initialization():
            if not source in self.context.rwos:
                missing_sources.append(source)

        if len(missing_sources) > 0:
            raise PhaserError(f"{len(missing_sources)} sources need initialization: {missing_sources}")

    def run(self):
        self.move_previous_file(self.working_dir / self.source_copy_filename())
        self.move_previous_file(self.errors_and_warnings_file)

        self.validate_sources()
        if self.source is None:
            raise ValueError("Pipeline source may not be None")
        next_source = self.source
        source_data_to_copy = Records(self.load(self.source))
        self.save(source_data_to_copy.for_save(), self.working_dir / self.source_copy_filename())

        for phase in self.phase_instances:
            destination = self.get_destination(phase)
            self.run_phase(phase, next_source, destination)
            next_source = destination

    def run_phase(self, phase, source, destination):
        self.context.current_phase = phase.name
        logger.info(f"Loading input from {source} for {phase.name}")
        data = Records(self.load(source))
        phase.load_data(data)
        try:
            results = phase.run()
        except Exception as exc:
            self.context.process_exception(exc, phase, 'None', None)
            if self.context.error_policy in [ON_ERROR_STOP_NOW, ON_ERROR_COLLECT]:
                raise PhaserError(f"Error in pipeline running {phase.name}") from exc

        if len(results) == 0:
            raise DataException(f"No rows left to process after phase {phase.name} - terminating early")
        self.save(results.for_save(), destination)
        self.check_extra_outputs(phase)
        self.save_extra_outputs()
        logger.info(f"{phase.name} saved output to {destination}")
        self.report_errors_and_warnings(phase.name)
        if self.context.phase_has_errors(phase.name):
            raise DataException(f"Phase '{phase.name}' failed with errors.")

    def report_errors_and_warnings(self, phase_name):
        """ TODO: different formats, flexibility:
        For CLI operation we want to report errors to the CLI, but for unsupervised operation these should go
        to logs.  Python logging does allow users of a library to send log messages to more than one place while
        customizing log level desired, and we could have drop-row messages as info and warning as warn level so
        these fit very nicely into the standard levels allowing familiar customization.  """
        with open(self.errors_and_warnings_file, 'a') as f:
            f.write("-------------\n")
            f.write(f"Beginning errors and warnings for {phase_name}\n")
            f.write("-------------\n")
            for row_num, event_list in self.context.events[phase_name].items():
                for event in event_list:
                    f.write(f"{event['type']} in step {event['step_name']}, row {row_num}: " +
                            f"message: '{event['message']}'\n")
                    if event['stack_info']:
                        f.write(event['stack_info'])

    def check_extra_outputs(self, phase):
        """ Check that any extra outputs the phase declared have been added into the context.
        Throws a PhaserError if any do not exist, as that is a programming error that should be fixed."""
        missing = []
        for output in phase.extra_outputs:
            if output.name not in self.context.rwos:
                missing.append(output.name)
        if len(missing) > 0:
            raise PhaserError(f"Phase {phase.name} missing extra_outputs: {missing}")

    def save_extra_outputs(self):
        for item in self.context.rwos.values():
            # Since context is passed from Phase to Phase, only save the new ones with to_save=True
            if item.to_save:
                filename = self.working_dir / f"{item.name}.csv"
                self.move_previous_file(filename)
                item.save(filename)
                logger.info(f"Extra output {item.name} saved to {self.working_dir}")
                item.to_save = False

    def load(self, source):
        """ The load method can be overridden to apply a pipeline-specific way of loading data.
        Phaser default is to read data from a CSV file. """
        return read_csv(source)

    def move_previous_file(self, file_path):
        if Path(file_path).is_file():
            # Move data from previous runs to snapshot dir
            if not self.prev_run_dir.is_dir():
                logger.debug(f"Moving files from previous runs to {self.prev_run_dir}")
                self.prev_run_dir.mkdir(exist_ok=False)
            os.rename(file_path, self.prev_run_dir / os.path.basename(os.path.normpath(file_path)))

    def save(self, results, destination):
        """ This method saves the result of the Phase operating on the batch, in phaser's preferred format.
        It should be easy to override this method to save in a different way, using pandas' to_csv, to_excel, to_json
        or a different output entirely.
        """
        self.move_previous_file(destination)
        save_csv(destination, results)

    @classmethod
    def source_copy_filename(cls):
        return "source_copy.csv"

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
