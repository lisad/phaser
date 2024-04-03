from abc import ABC, abstractmethod
from copy import deepcopy
import pandas as pd
import logging
from .column import make_strict_name, Column
from .pipeline import (Pipeline, Context, DropRowException, WarningException, PhaserError,
                       PHASER_ROW_NUM)
from .records import Records, Record
from .steps import ROW_STEP, BATCH_STEP, CONTEXT_STEP, PROBE_VALUE, row_step, DATAFRAME_STEP

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


class PhaseBase(ABC):

    def __init__(self, name, steps=None, context=None, error_policy=None):
        self.name = name or self.__class__.__name__
        self.context = context or Context()
        self.steps = steps or self.__class__.steps
        self.error_policy = error_policy or Pipeline.ON_ERROR_COLLECT
        self.headers = None
        self.row_data = None

    def load_data(self, data):
        """ Call this method to pass record-oriented data to the Phase before calling 'run'
        Can be overridden to load data in a different structure.
        Used in phaser's builtin phases - by regular Phase and ReshapePhase.
        Note that in normal operation, a Records object is passed in with Record objects and row numbers -
        however if a Phase is being used in tests, it makes testing a lot easier if load_data can take a
        raw list of dicts and row numbers get added.  """
        if isinstance(data, pd.DataFrame):
            self.headers = data.columns.values.tolist()
            data = data.to_dict('records')

        if isinstance(data, Records):
            self.headers = data.headers
            self.row_data = data
        elif isinstance(data, list):
            if len(data) > 0 and self.headers is None:
                self.headers = data[0].keys()
            self.row_data = Records(data)
        else:
            raise PhaserError(f"Phase load_data called with unsupported data format {data.__class__}")

    @abstractmethod
    def run(self):
        """ Each kind of phase has a different process for doing its work, so this method must
        be overridden.  """
        pass

    def run_steps(self):
        if self.row_data is None or self.row_data == []:
            raise PhaserError("No data loaded yet")
        for step in self.steps:
            step_type = step(None, __probe__=PROBE_VALUE)
            if step_type == ROW_STEP:
                self.execute_row_step(step)
            elif step_type == BATCH_STEP:
                self.execute_batch_step(step)
            elif step_type == DATAFRAME_STEP:
                self.execute_batch_step(step)
            elif step_type == CONTEXT_STEP:
                self.execute_context_step(step)
            else:
                raise PhaserError(f"Unknown step type {step_type}")

    def execute_row_step(self, step):
        """ Internal method. Each step that is run on a row is run through this method in order to do consistent error
        numbering and error reporting.
        """
        new_data = Records(number_from=next(self.row_data.row_num_gen))
        for row_index, row in enumerate(self.row_data):
            if row.row_num in self.context.errors.keys():
                # LMDTODO: This is an O(n) operation.  If instead the fact of the row having an error was part of the
                # row data, this would be O(1).  We should probably do that instead, and definitely if any row metadata
                # is on the row besides the original row number.  The thing in the special field in row could be an obj.
                continue    # Only trap the first error per row
            # NOw that we know the row number run the step and handle exceptions.
            try:
                new_row = step(deepcopy(row), context=self.context)
                if isinstance(new_row, Record):
                    # Ensure the original row_num is preserved with the new row returned from the step
                    if new_row.row_num != row.row_num:
                        raise PhaserError(f"Row number {row.row_num} changed to {new_row.row_num} during row_step")
                    new_data.append(new_row)
                else:
                    # LMDTODO: write a test that confirms we can just return a new dict from a step
                    new_data.append(Record(row.row_num, new_row))
            except Exception as exc:
                self.process_exception(exc, step, row)
                if not isinstance(exc, DropRowException):
                    new_data.append(row)  # If we are continuing, keep the row in the data unchanged unless it's a
                    # DropRowException. (If the caller wants to change the row and also throw an exception, they can't)
        self.row_data = new_data

    def execute_batch_step(self, step):
        try:
            new_row_values = step(self.row_data, context=self.context)
            row_size_diff = len(self.row_data) - len(new_row_values)
            if row_size_diff > 0:
                self.context.add_warning(step, None, f"{row_size_diff} rows were dropped by step")
            elif row_size_diff < 0:
                self.context.add_warning(step, None, f"{abs(row_size_diff)} rows were ADDED by step")
            preserve_row_num = next(self.row_data.row_num_gen)
            self.row_data = Records([row for row in new_row_values], number_from=preserve_row_num)
        except Exception as exc:
            self.process_exception(exc, step, None)

    def execute_context_step(self, step):
        try:
            step(self.context)
        except DropRowException as dre:
            raise PhaserError("DropRowException can't be handled in a context_step") from dre
        except Exception as exc:
            self.process_exception(exc, step, None)

    def process_exception(self, exc, step, row):
        """
        A method to delegate exception handling to.  This is not called within PhaseBase directly,
        but it is called in the subclasses when they run steps or methods.
        :param exc: The exception or error thrown
        :param step: What step this occurred in
        :param row: What row of the data this occurred in
        :return: Nothing
        """
        if isinstance(exc, PhaserError):
            # PhaserError is raised in case of coding contract issues, so should bypass data exception handling.
            raise exc
        elif isinstance(exc, DropRowException):
            self.context.add_dropped_row(step, row, exc.message)
        elif isinstance(exc, WarningException):
            self.context.add_warning(step, row, exc.message)
        else:
            e_name = exc.__class__.__name__
            e_message = str(exc)
            # TODO: Would be nice to include some traceback information in the
            # recorded error as well.
            message = f"{e_name} raised ({e_message})" if e_message else f"{e_name} raised."
            logger.debug(f"Unknown exception handled in executing steps ({message}")

            match self.error_policy:
                case Pipeline.ON_ERROR_COLLECT:
                    self.context.add_error(step, row, message)
                case Pipeline.ON_ERROR_WARN:
                    self.context.add_warning(step, row, message)
                case Pipeline.ON_ERROR_DROP_ROW:
                    self.context.add_dropped_row(step, row, message)
                case Pipeline.ON_ERROR_STOP_NOW:
                    self.context.add_error(step, row, message)
                    raise exc
                case _:
                    raise PhaserError(f"Unknown error policy '{self.error_policy}'") from exc


class ReshapePhase(PhaseBase):
    """ Operations that combine rows or split rows, and thus arrive at a different number of rows (beyond just
    dropping bad data), don't work well in a regular Phase and are hard to do diffs for.  This class solves
    just the problem of merging and splitting up rows.  Some reshape operations include
    * group by a field (and sum, or average, or apply another operation to the numeric fields associated)
    * 'spread' functions

    Note that just dropping or filtering rows one-by-one, or adding or removing columns no matter how much
    other column values are involved, can be done in a regular phase, with the additional features like 'diff'
    that a regular phase provides.
    """

    def __init__(self, name, steps=None, context=None, error_policy=None):
        super().__init__(name, steps=steps, context=context, error_policy=error_policy)

    def run(self):
        # Break down run into load, steps, error handling, save and delegate
        self.run_steps()
        return self.row_data


class Phase(PhaseBase):
    """ The organizing principle for data transformation steps and column definitions is the phase.  A phase can

    * load a data file
    * Apply a set of preferred column names and datatypes via 'columns'
    * Apply a further list of transformations via 'steps'
    * While applying steps, can drop invalid or unwanted rows, add columns
    * Save only the desired columns
    * Provide a detailed diff or a summary of what changed in the phase

    Attributes
    ----------
    name : str
        The name of the phase (for debugging and file name usage)
    steps : list
        A list of functions that will be run in order on data loaded into the phase
    columns : list
        A list of column definitions with declarations of how to handle the column name and data within
        the column. Columns are also processed in order, so a column early in the list that instructs the
        phase to drop rows without values will cause those rows never to be processed by columns later in the
        list.
    context : Context obj
        Optional context information that can apply to multiple phases organized in a Pipeline.  If
        no context is passed in, one will be created just for this Phase. The context will be passed to each step
        in case that step needs outside context.
    error_policy: str
        The error handling policy to apply in this phase.  Default is Pipeline.ON_ERROR_COLLECT, which collects
        errors, up to one per row, and reports all errors at the end of running the phase.  Other options
        are Pipeline.ON_ERROR_WARN, which adds warnings that will all be reported at the end,
        Pipeline.ON_ERROR_DROP_ROW which means that a row causing an error will be dropped, and
        Pipeline.ON_ERROR_STOP_NOW which aborts the phase mid-step rather than continue and collect more errors.
        Any step that needs to apply different error handling than the phase's default can throw its own
        typed exception (see step documentation).


    Methods
    -------
    run(source, destination)
        Loads data from source, applies all the phase's column definitions and steps, and saves to destination.
        If run inside a Pipeline, the pipeline will call this, but for debugging/developing or simpler data
        transformations, this can be used to run the phase without a Pipeline.

    load(source)
        If creating a Phase that takes data in a custom way, subclass Phase and override the load method.
        Besides overriding the load method, users of Phase should not need to run load directly as it is run
        as part of 'run'. if overriding 'load', make sure that both phase.headers and phase.row_data are
        set up before finishing the method.

    save(source)
        If creating a Phase that sends data to a custom destination, subclass Phase and override the save method.
        If the method is not overridden, the phase will save the data in CSV format at the destination.

    """
    source = None
    working_dir = None
    steps = []
    columns = []

    def __init__(self, name=None, steps=None, columns=None, context=None, error_policy=None):
        """ Instantiate (or subclass) a Phase with an ordered list of steps (they will be called in this order) and
        with an ordered list of columns (they will do their checks and type casting in this order).  """
        super().__init__(name, steps=steps, context=context, error_policy=error_policy)
        self.columns = columns or self.__class__.columns
        if isinstance(self.columns, Column):
            self.columns = [self.columns]

        self.row_data = None
        self.headers = None

    def run(self):
        # Break down run into load, steps, error handling, save and delegate
        self.do_column_stuff()
        self.run_steps()
        self.prepare_for_save()
        return self.row_data

    def do_column_stuff(self):
        @row_step
        def cast_each_column_value(row, context):
            """ We run this as a row step to have consistent error handling and DRY.  It could be
            a little better at reporting which column generated the error.  The fact that it quits after the first
            raised error (within one row) is intentional especially so the row can be dropped after the first
            error.  Columns are processed in declared order so that a fundamental check can be done before
            columns that assume previous checks (e.g. a "type" column drops bad rows and subsequent columns
            can assume the correct type). """
            new_row = row
            for col in self.columns:
                new_row = col.check_and_cast_value(new_row)
            return new_row

        # Header work is done first
        self.rename_columns()
        for column in self.columns:
            column.check_required(self.headers)
        # Then going row by row allows us to re-use row-based error/reporting work
        self.execute_row_step(cast_each_column_value)

    def rename_columns(self):
        """ Renames columns: both using case and space ('_', ' ') matching to convert columns to preferred
        label format, and using a list of additional alternative names provided in each column definition.
        It would be cool if this could be done before converting everything to list-of-dicts format...
        """
        rename_list = {alt: col.name for col in self.columns for alt in col.rename}
        strict_name_list = {make_strict_name(col.name): col.name for col in self.columns}

        def rename_me(name):
            name = name.strip()
            if name.startswith('"') and name.endswith('"'):
                name = name.strip('"')
            if make_strict_name(name) in strict_name_list.keys():
                name = strict_name_list[make_strict_name(name)]  # Convert to declared capital'n/separ'n
            if name in rename_list.keys():
                name = rename_list[name]  # Do declared renames
            return name

        for row in self.row_data:
            if None in row.keys():
                # This check for keys named None should maybe be done in read_csv or at least in pipeline.
                # It's IO relaetd - it can happen if a row has extra commas compared to the header line
                self.context.add_warning('__phaser_rename_columns',
                                         row,
                                         f"Extra value found in row, may mis-align other values")
                del row[None]

            # We're resetting the data in the whole Record to achieve renaming ... but keeping the row number
            row.data = {rename_me(key): value for key, value in row.items()}

        self.headers = [rename_me(name) for name in self.headers if name is not None]

    def prepare_for_save(self):
        """ Checks consistency of data and drops unneeded columns
        """
        self.check_headers_consistent()
        columns_to_drop = [col.name for col in self.columns if col.save is False]
        if len(columns_to_drop) == 0:
            # Nothing to do, so bail fast
            return
        for row in self.row_data:
            for col in columns_to_drop:
                if col in row:
                    del row[col]

    def check_headers_consistent(self):
        for row in self.row_data:
            for field_name in row.keys():
                if field_name not in self.headers:
                    # TODO: Fix -- context adds warnings to the 'current_row'
                    # record, not the record associated with the row passed in
                    # here. In this method, all of the errors are logged on the
                    # last row of the data, because current_row is not changed.
                    self.context.add_warning('consistency_check', row,
                        f"New field '{field_name}' was added to the row_data and not declared a header")

