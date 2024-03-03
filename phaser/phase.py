from abc import ABC, abstractmethod
from collections import UserDict, UserList
import pandas as pd
import logging
from .column import make_strict_name, Column
from .pipeline import Pipeline, Context, DropRowException, WarningException, PipelineErrorException, PhaserException
from .steps import ROW_STEP, BATCH_STEP, CONTEXT_STEP, PROBE_VALUE, row_step

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


class PhaseBase(ABC):

    def __init__(self, name, context=None, error_policy=None):
        self.name = name or self.__class__.__name__
        self.context = context or Context()
        self.error_policy = error_policy or Pipeline.ON_ERROR_COLLECT
        self.headers = None
        self.row_data = None

    def load_data(self, data):
        """ Call this method to pass record-oriented data to the Phase before calling 'run'
        Can be overridden to load data in a different structure.
        Used in phaser's builtin phases - by regular Phase and ReshapePhase. """
        if isinstance(data, pd.DataFrame):
            self.headers = data.columns.values.tolist()
            data = data.to_dict('records')
        if isinstance(data, list):
            if len(data) > 0 and self.headers is None:
                self.headers = data[0].keys()
            self.row_data = PhaseRecords(data)
        else:
            raise PhaserException("Phase load_data called with unsupported data format")

    @abstractmethod
    def run(self):
        """ Each kind of phase has a different process for doing its work, so this method must
        be overridden.  """
        pass

    def process_exception(self, exc, step, row):
        """
        A method to delegate exception handling to.  This is not called within PhaseBase directly,
        but it is called in the subclasses when they run steps or methods.
        :param exc: The exception or error thrown
        :param step: What step this occurred in
        :param row: What row of the data this occurred in
        :return: Nothing
        """
        if isinstance(exc, DropRowException):
            self.context.add_dropped_row(step, row, exc.message)
        elif isinstance(exc, WarningException):
            self.context.add_warning(step, row, exc.message)
        else:
            e_name = exc.__class__.__name__
            e_message = str(exc)
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
                    raise PipelineErrorException(f"Unknown error policy '{self.error_policy}'") from exc

    def report_errors_and_warnings(self):
        """ In next iteration, we should probably move the generation of error info to stdout to other locations.
        For CLI operation we want to report errors to the CLI, but for unsupervised operation these should go
        to logs.  Python logging does allow users of a library to send log messages to more than one place while
        customizing log level desired, and we could have drop-row messages as info and warning as warn level so
        these fit very nicely into the standard levels allowing familiar customization.  """
        # LMDTODO: How are one phases's errors and warnings kept separate from another phase's?
        for row_num, info in self.context.dropped_rows.items():
            print(f"DROPPED row: {row_num}, message: '{info['message']}'")
        # Unlike errors and dropped rows, there can be multiple warnings per row
        for row_num, warnings in self.context.warnings.items():
            for warning in warnings:
                print(f"WARNING row: {row_num}, message: '{warning['message']}'")
        for row_num, error in self.context.errors.items():
            print(f"ERROR row: {row_num}, message: '{error['message']}'")


class DataFramePhase(PhaseBase):
    def __init__(self, name, context=None, error_policy=None):
        super().__init__(name, context=context, error_policy=error_policy)
        self.df_data = None

    def run(self):
        self.df_data = self.df_transform(self.df_data)
        self.report_errors_and_warnings()
        return self.df_data.to_dict('records')

    @abstractmethod
    def df_transform(self, df_data):
        """ The df_transform method is implemented in subclasses of DataFramePhase.  Significant reshaping can be
        done because data is not processed row-by-row, and row numbers are not reported on in error reporting.
        """
        raise PhaserException("Subclass DataFramePhase and return a new dataframe in the 'df_transform' method")

    def load_data(self, data):
        # Overrides the regular load_data because we just want to accept dataframe and keep it in df format.
        self.df_data = data


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

    def __init__(self, name, context=None, error_policy=None):
        super().__init__(name, context=context, error_policy=error_policy)

    @abstractmethod
    def reshape(self, row_data):
        """ When ReshapePhase is implemented for a pipeline, this method takes a list of rows (as dicts) and returns
        a new list of rows (as dicts), which could have a very different number of rows and/or columns.  The
        rest of the class takes care of loading, saving, and reporting errors and warnings.
        """
        raise PhaserException("Subclass ReshapePhase and return new data version in this reshape method")

    def run(self):
        self.row_data = self.reshape(self.row_data)
        self.report_errors_and_warnings()
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
        super().__init__(name, context=context, error_policy=error_policy)
        self.steps = steps or self.__class__.steps
        self.columns = columns or self.__class__.columns
        if isinstance(self.columns, Column):
            self.columns = [self.columns]

        self.row_data = PhaseRecords()
        self.headers = None

    def run(self):
        # Break down run into load, steps, error handling, save and delegate
        self.do_column_stuff()
        self.run_steps()
        self.report_errors_and_warnings()
        self.prepare_for_save()
        return self.row_data.to_records()

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
            if make_strict_name(name) in strict_name_list.keys():
                name = strict_name_list[make_strict_name(name)]  # Convert to declared capital'n/separ'n
            if name in rename_list.keys():
                name = rename_list[name]  # Do declared renames
            return name

        self.row_data = PhaseRecords([{rename_me(key): value for key, value in row.items()} for row in self.row_data])
        self.headers = [rename_me(name) for name in self.headers]

    def prepare_for_save(self):
        """ Checks consistency of data and drops unneeded columns
        """
        self.check_headers_consistent()
        # Use the raw list(dict) form of the data, because DataFrame
        # construction does something different with a subclass of Sequence and
        # Mapping that results in the columns being re-ordered.
        df = pd.DataFrame(self.row_data.to_records())
        columns_to_drop = [col.name for col in self.columns if col.save is False]
        columns_exist_to_drop = [col_name for col_name in columns_to_drop if col_name in df.columns]
        df.drop(columns_exist_to_drop, axis=1, inplace=True)
        self.row_data = PhaseRecords(df.to_dict('records'))

    def check_headers_consistent(self):
        for row in self.row_data:
            for field_name in row.keys():
                if field_name not in self.headers:
                    self.context.add_warning('consistency_check', row.row_num,
                        f"At some point, {field_name} was added to the row_data and not declared a header")

    def run_steps(self):
        if self.row_data is None or self.row_data == []:
            raise Exception("No data loaded yet")
        for step in self.steps:
            step_type = step(None, __probe__=PROBE_VALUE)
            if step_type == ROW_STEP:
                self.execute_row_step(step)
            elif step_type == BATCH_STEP:
                self.execute_batch_step(step)
            elif step_type == CONTEXT_STEP:
                self.execute_context_step(step)
            else:
                raise Exception(f"Unknown step type {step_type}")

    def execute_row_step(self, step):
        """ Internal method. Each step that is run on a row is run through this method in order to do consistent error
        numbering and error reporting.
        """
        new_data = PhaseRecords()
        for row_index, row in enumerate(self.row_data):
            self.context.current_row = row.row_num

            if self.context.current_row in self.context.errors.keys():
                # LMDTODO: This is an O(n) operation.  If instead the fact of the row having an error was part of the
                # row data, this would be O(1).  We should probably do that instead, and definitely if any row metadata
                # is on the row besides the original row number.  The thing in the special field in row could be an obj.
                continue    # Only trap the first error per row
            # NOw that we know the row number run the step and handle exceptions.
            try:
                # LMDTODO: pass a deepcopy of row
                new_row = step(row, context=self.context)
                # Ensure the original row_num is preserved with the new row
                # returned from the step
                if isinstance(new_row, PhaseRecord):
                    new_row.row_num = self.context.current_row
                    new_data.append(new_row)
                else:
                    new_data.append(PhaseRecord(self.context.current_row, new_row))
            except Exception as exc:
                self.process_exception(exc, step, row)
                if not isinstance(exc, DropRowException):
                    new_data.append(row)  # If we are continuing, keep the row in the data unchanged unless it's a
                    # DropRowException. (If the caller wants to change the row and also throw an exception, they can't)
        self.row_data = new_data

    def execute_batch_step(self, step):
        self.context.current_row = 'batch'
        try:
            new_row_values = step(self.row_data, context=self.context)
            row_size_diff = len(self.row_data) - len(new_row_values)
            if row_size_diff > 0:
                self.context.add_warning(step, None, f"{row_size_diff} rows were dropped by step")
            elif row_size_diff < 0:
                self.context.add_warning(step, None, f"{abs(row_size_diff)} rows were ADDED by step")
            self.row_data = PhaseRecords([row for row in new_row_values])
        except Exception as exc:
            self.process_exception(exc, step, None)

    def execute_context_step(self, step):
        self.context.current_row = 'context'
        try:
            step(self.context)
        except DropRowException as dre:
            raise PhaserException("DropRowException can't be handled in a context_step") from dre
        except Exception as exc:
            self.process_exception(exc, step, None)


# LMDTODO: add a test that makes sure that a batch step followed by a row step works fine


class PhaseRecords(UserList):
    def __init__(self, *args):
        super().__init__(*args)
        self.data = [
            PhaseRecord(index, record)
            for index, record in enumerate(self.data)
        ]

    # Transform back into native list(dict)
    def to_records(self):
        return [ r.data for r in self.data ]


class PhaseRecord(UserDict):
    def __init__(self, row_num, record):
        super().__init__(record)
        self.row_num = row_num
