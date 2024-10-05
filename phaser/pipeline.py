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
        self.name = name
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
        self.check_output_collision()

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

    def expected_outputs(self):
        """ All the files expected to be saved in the pipeline.  Right now this has
        phase output checkpoint files, extra outputs, errors and warnings file and the
        copy of source used for diffs.
        """
        expected_outputs = [self.phase_save_filename(phase) for phase in self.phase_instances]
        expected_outputs.append(self.source_copy_filename())
        expected_outputs.append(self.errors_and_warnings_filename())
        for phase in self.phase_instances:
            expected_outputs.extend([self.item_save_filename(item) for item in phase.extra_outputs])
        return expected_outputs

    def check_output_collision(self):
        """ This is to check that the outputs of the pipeline are not going to
        overwrite the source file or each other, and
        that previous copies of the outputs are copied to a previous-run directory """
        expected_outputs = self.expected_outputs()
        if len(set(expected_outputs)) != len(expected_outputs):
            raise PhaserError("One of the filenames expected to be saved overlaps with another.  ("
                              + ", ".join(sorted(expected_outputs)) + ")")

        if (os.path.basename(self.source) in expected_outputs and
                os.path.dirname(self.source) == self.working_dir):
            raise PhaserError("One of the expected outputs will overwrite the source file.  ("
                              + ", ".join(sorted(expected_outputs)) + ")")

    def cleanup_working_dir(self):
        timestamp = None
        if Path(self.errors_and_warnings_file()).is_file():
            with open(self.errors_and_warnings_file(), 'r') as f:
                timestamp = f.readline()
        if not timestamp or len(timestamp) != 14:
            timestamp = datetime.today().strftime("%y%m%d-%H%M%S")

        prev_run_dir = Path(self.working_dir / f"{self.name}-{timestamp}")
        logger.debug(f"Moving files from previous run to {prev_run_dir}")
        prev_run_dir.mkdir(exist_ok=False)

        for filename in self.expected_outputs():
            file_path = self.working_dir / filename
            if Path(file_path).is_file():
                os.rename(file_path, prev_run_dir / os.path.basename(os.path.normpath(file_path)))

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
        """ Nothing should be saved to file during instantiation, in case Pipeline is instantiated for
        another reason such as inspection.  Thus, we do file cleanup/setup only at the start of 'run'."""
        self.cleanup_working_dir()
        with open(self.errors_and_warnings_file(), 'a') as f:
            f.write(datetime.today().strftime("%y%m%d-%H%M%S") + '\n')

        self.validate_sources()
        if self.source is None:
            raise ValueError("Pipeline source may not be None")
        next_source = self.source
        source_data_to_copy = Records(self.load(self.source))
        self.save(source_data_to_copy.for_save(), self.working_dir / self.source_copy_filename())

        for phase in self.phase_instances:
            destination = self.working_dir / self.phase_save_filename(phase)
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
        with open(self.errors_and_warnings_file(), 'a') as f:
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
                item.save(filename)
                logger.info(f"Extra output {item.name} saved to {self.working_dir}")
                item.to_save = False

    @classmethod
    def load(cls, source):
        """ The load method can be overridden to apply a pipeline-specific way of loading data.
        Phaser default is to read data from a CSV file. """
        return read_csv(source)

    def save(self, results, destination):
        """ This method saves the result of the Phase operating on the batch, in phaser's preferred format.
        It should be easy to override this method to save in a different way, using pandas' to_csv, to_excel, to_json
        or a different output entirely.
        """
        save_csv(destination, results)

    @classmethod
    def phase_save_filename(cls, phase):
        """
        As a class method, this can be called from the diffing tool which would like to know what
        names files will be saved in this pipeline.
        """
        if inspect.isclass(phase):
            phase = phase()
        return f"{phase.name}_output.csv"

    @classmethod
    def item_save_filename(cls, item):
        return f"{item.name}.csv"

    @classmethod
    def source_copy_filename(cls):
        return "source_copy.csv"

    @classmethod
    def errors_and_warnings_filename(cls):
        return 'errors_and_warnings.txt'

    def errors_and_warnings_file(self):
        return self.working_dir / self.errors_and_warnings_filename()