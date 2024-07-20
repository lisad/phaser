import traceback
import logging
from collections import defaultdict

from .exceptions import *
from .constants import *
from .io import IOObject
from .records import Record


logger = logging.getLogger(__name__)


class Context:
    """ Context is created by the pipeline, and passed to each phase.  Thus, it can be used
    to carry extra data or variable values between phases if necessary. """

    ERROR = "ERROR"
    WARNING = "WARNING"
    DROPPED_ROW = "DROPPED_ROW"

    def __init__(self, variables=None, working_dir=None, error_policy=ON_ERROR_COLLECT, verbose=False):
        self.verbose = verbose
        # Messages or events (errors, warnings, etc) will be indexed by phase, then row number.  The type (error,
        # warning, or dropped row) will be a type on the dict of event information
        self.events = defaultdict(lambda: defaultdict(list))
        self.current_phase = 'Unknown'
        self.variables = variables or {}
        self.current_row = None
        # Stores sources and outputs as ReadWriteObjects
        self.rwos = {}
        self.working_dir = working_dir
        self.error_policy = error_policy or ON_ERROR_COLLECT

    def _get_phase_name(self, phase):
        if phase is None:
            return self.current_phase
        return phase.name

    def add_event(self, event_info):
        if event_info['type'] not in [Context.ERROR, Context.WARNING, Context.DROPPED_ROW]:
            raise PhaserError("Error or other row event not correct type")
        event_info['row_num'] = _extract_row_num(event_info['row'])
        event_info['step_name'] = _stringify_step(event_info.pop('step'))
        event_info['phase_name'] = self._get_phase_name(event_info.pop('phase'))
        self.events[event_info['phase_name']][event_info['row_num']].append(event_info)

    def add_error(self, step, row, message, stack_info=None, phase=None):
        self.add_event({
            'type': Context.ERROR,
            'phase': phase,
            'step': step,
            'row': row,
            'message': message,
            'stack_info': stack_info
        })

    def add_warning(self, step, row, message, stack_info=None, phase=None):
        self.add_event({
            'type': Context.WARNING,
            'phase': phase,
            'step': step,
            'row': row,
            'message': message,
            'stack_info': stack_info
        })

    def add_dropped_row(self, step, row, message, stack_info=None, phase=None):
        self.add_event({
            'type': Context.DROPPED_ROW,
            'phase': phase,
            'step': step,
            'row': row,
            'message': message,
            'stack_info': stack_info
        })

    def row_has_errors(self, row_num):
        for phase, events in self.events.items():
            if row_num in events and any(event['type'] == Context.ERROR for event in events[row_num]):
                return True
        return False

    def phase_has_errors(self, phase_name):
        if phase_name not in self.events.keys():
            raise PhaserError(f"Pass in a phase name to look up errors, {phase_name} not found in context events")
        for event_list in self.events[phase_name].values():
            if any(event['type'] == Context.ERROR for event in event_list):
                return True
        return False

    def get_events(self, phase=None, row_num=None):
        if row_num and phase:
            return self.events[phase.name][row_num]
        elif phase:
            return self.events[phase.name]
        else:
            return self.events

    def add_variable(self, name, value):
        """ Add variables that are global to the pipeline and accessible to steps and internal methods """
        self.variables[name] = value

    def get(self, name):
        return self.variables.get(name)

    def set_output(self, name, output):
        # At present outputs must be in record format and save to CSV, but this should be expanded.
        if name in self.rwos:
            logger.warning("Overwriting while adding output '%s'", name)
        if not isinstance(output, IOObject):
            raise PhaserError(f"outputs must be set to an IOObject. set_output({name}, {output})")
        output.to_save = True
        self.rwos[name] = output

    def set_source(self, name, source):
        if name in self.rwos:
            logger.warning("Overwriting while setting source '%s'", name)
        source.to_save = False
        self.rwos[name] = source

    def get_source(self, name):
        if name in self.rwos:
            return self.rwos[name].data
        raise PhaserError(f"Source not loaded before being used: {name}")

    def process_exception(self, exc, phase, step, row):
        """
        A method to delegate exception handling to turn into error reporting in standardized way.  Called by
        phase's step handlers when a phaser data exception or a coding exception occurs
        :param exc: The exception or error thrown
        :param step: What step this occurred in
        :param row: What row of the data this occurred in
        :param error_policy: The phase's chosen error handling policies (ON_ERROR_COLLECT, ON_ERROR_STOP_NOW, etc.)
        :return: Nothing
        """

        # PhaserError is raised in case of coding contract issues, so should bypass the error handling that's
        # appropriate for errors in data.
        if isinstance(exc, PhaserError):
            raise exc

        e_name = exc.__class__.__name__
        e_message = str(exc)
        message = f"{e_name} raised ({e_message})" if e_message else f"{e_name} raised."
        logger.warning(f"Unknown exception handled in executing steps ({message}")
        stack_info = traceback.format_exc() if self.verbose else None
        event_info = {'phase': phase,
                      'step': step,
                      'row': row,
                      'message': message,
                      'stack_info': stack_info
                    }

        # If the exception tells us how to handle it, that's more specific so do that first.
        if isinstance(exc, DropRowException):
            self.add_event({'type': Context.DROPPED_ROW, **event_info})
        elif isinstance(exc, WarningException):
            self.add_event({'type': Context.WARNING, **event_info})

        # Otherwise, decide how to handle exception based on error_policy
        elif self.error_policy == ON_ERROR_COLLECT:
            self.add_event({'type': Context.ERROR, **event_info})
        elif self.error_policy == ON_ERROR_WARN:
            self.add_event({'type': Context.WARNING, **event_info})
        elif self.error_policy == ON_ERROR_DROP_ROW:
            self.add_event({'type': Context.DROPPED_ROW, **event_info})
        elif self.error_policy == ON_ERROR_STOP_NOW:
            self.add_event({'type': Context.ERROR, **event_info})
            logger.exception(f"Stopping the pipeline; error policy: {self.error_policy}")
            raise exc
        else:
            raise PhaserError(f"Unknown error policy '{self.error_policy}'") from exc


def _extract_row_num(row):
    if not row:
        return 'none'
    if isinstance(row, Record):
        return row.row_num
    if isinstance(row, dict):
        return row.get(PHASER_ROW_NUM, 'unknown')
    raise PhaserError("Unrecognized data type for row (can handle Record or dict)")


def _stringify_step(step):
    if isinstance(step, str):
        return step
    try:
        return getattr(step, '__name__')
    except Exception as e:
        logger.error(f"Unknown case trying to turn {step} into a step name")
        raise e
