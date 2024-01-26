import pandas as pd
import logging
from .column import make_strict_name
from .pipeline import Pipeline, Context, DropRowException, WarningException, PipelineErrorException
from .steps import ROW_STEP, BATCH_STEP, DATAFRAME_STEP, PROBE_VALUE, row_step

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


class Phase:
    # Subclasses can override to set source for all instances if appropriate
    source = None
    working_dir = None
    steps = []
    columns = []

    def __init__(self, name=None, steps=None, columns=None, context=None, error_policy=None):
        """ Instantiate (or subclass) a Phase with an ordered list of steps (they will be called in this order) and
        with an ordered list of columns (they will do their checks and type casting in this order).  """
        self.name = name or self.__class__.__name__
        self.steps = steps or self.__class__.steps
        self.columns = columns or self.__class__.columns
        if not context:
            # If a phase is being run in isolation and without the pipeline context, create a new one
            self.context = Context()

        self.row_data = []
        self.headers = None
        self.dataframe_data = None
        self.default_error_policy = error_policy or Pipeline.ON_ERROR_COLLECT

    def run(self, source, destination):
        # Break down run into load, steps, error handling, save and delegate
        self.load(source)
        self.do_column_stuff()
        self.run_steps()
        if self.context.has_errors():
            self.report_errors_and_warnings()
            raise PipelineErrorException(f"Phase failed with {len(self.context.errors.keys())} errors.")
        else:
            self.report_errors_and_warnings()
            self.save(destination)

    def report_errors_and_warnings(self):
        """ In next iteration, we should probably move the generation of error info to stdout to other locations.
        For CLI operation we want to report errors to the CLI, but for unsupervised operation these should go
        to logs.  Python logging does allow users of a library to send log messages to more than one place while
        customizing log level desired, and we could have drop-row messages as info and warning as warn level so
        these fit very nicely into the standard levels allowing familiar customization.  """
        for row_num, info in self.context.dropped_rows.items():
            print(f"DROPPED row: {row_num}, message: '{info['message']}'")
        for row_num, warning in self.context.warnings.items():
            print(f"WARNING row: {row_num}, message: '{warning['message']}'")
        for row_num, error in self.context.errors.items():
            print(f"ERROR row: {row_num}, message: '{error['message']}'")

    def load(self, source):
        """ When creating a Phase, it may be desirable to subclass it and override the load()
        function to do a different kind of loading.  Be sure to load the row_data into the
        instance's row_data attribute as an iterable (list) containing dicts.
        Defaults:
        * assume all column values are strings, so leading zeros or trailing zeros don't get destroyed.
        * assume ',' value-delimiter
        * skip_blank_lines=True: allows blank AND '#'-led rows to be skipped and still find header row
        * doesn't use indexing
        * does attempt to decompress common compression formats if file uses them
        * assume UTF-8 encoding
        * uses '#' as the leading character to assume a row is comment
        * Raises errors loading 'bad lines', rather than skip
        """
        logger.info(f"{self.name} loading input from {source}")
        df = pd.read_csv(source,
                         dtype='str',
                         sep=',',
                         skip_blank_lines=True,
                         index_col=False,
                         comment='#')
        self.headers = df.columns.values.tolist()
        if Pipeline.ROW_NUM_FIELD in df.columns.values.tolist():
            raise Exception("phaser does not currently expect __phaser_row_num__ to be saved and loaded")
            # LMDTODO: Or could check that the rows are correctly numbered or can be ordered by number and unique?
            # this would preserve original row numbers across data.  can't be done in reshape phases.
        else:
            df[Pipeline.ROW_NUM_FIELD] = df.reset_index().index
        self.dataframe_data = df
        self.row_data = df.to_dict('records')

    def load_data(self, data):
        """ Alternate load method is useful in tests or in scripting Phase class where the data is not in a file.
        This assumes data is in the form of a list of dicts where dicts have consistent keys (e.g. pandas 'record'
        format) """
        if len(data) > 0:
            self.headers = data[0].keys()
        self.row_data = data

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

        self.row_data = [{rename_me(key): value for key, value in row.items()} for row in self.row_data]
        self.headers = [rename_me(name) for name in self.headers]

    def save(self, destination):
        """ This method saves the result of the Phase operating on the batch in phaser's preferred approach.
        It should be easy to override this method to save in a different way, using different
        parameters on pandas' to_csv, or to use pandas' to_excel, to_json or a different output entirely.
        Defaults:
        separator character is ','
        encoding is UTF-8
        compression will be attempted if filename ends in 'zip', 'gzip', 'tar' etc
        """
        # LMDTODO: Synch the dataframe version every step rather than just on save - issue 13
        self.check_headers_consistent()

        self.dataframe_data = pd.DataFrame(self.row_data)
        # LMDTODO: Should saving row numbers be an option?
        self.dataframe_data.drop(Pipeline.ROW_NUM_FIELD, axis='columns', inplace=True)
        self.dataframe_data.to_csv(destination,
                                   index=False,
                                   na_rep="NULL",   # LMDTODO Reconsider: this makes checkpoints more readable
                                                    # but may make final import harder
                                   )
        logger.info(f"{self.name} saved output to {destination}")

    def check_headers_consistent(self):
        for row in self.row_data:
            for field_name in row.keys():
                if field_name not in self.headers and field_name != '__phaser_row_num__':
                    raise Exception(f"At some point, {field_name} was added to the row_data and not declared a header")
        # LMDTODO: We could also check for fields dropped in row_data steps and add them if they can be null?

    def run_steps(self):
        if self.row_data is None or self.row_data == []:
            raise Exception("No data loaded yet")
        for step in self.steps:
            step_type = step(None, __probe__=PROBE_VALUE)
            if step_type == ROW_STEP:
                self.execute_row_step(step)
            elif step_type == BATCH_STEP:
                self.execute_batch_step(step)
            elif step_type == DATAFRAME_STEP:
                raise Exception("Not implemented yet")
            else:
                raise Exception(f"Unknown step type {step_type}")

    def execute_row_step(self, step):
        """ Internal method. Each step that is run on a row is run through this method in order to do consistent error
        numbering and error reporting.
        """
        new_data = []
        for row_index, row in enumerate(self.row_data):
            # In proper execution, row data has already been enriched with original row numbers.
            # In tests or under scripting, row numbers may not have been set so include now for future steps.
            self.context.current_row = row.get(Pipeline.ROW_NUM_FIELD)
            if self.context.current_row is None:
                row[Pipeline.ROW_NUM_FIELD] = row_index
                self.context.current_row = row.get(Pipeline.ROW_NUM_FIELD)

            if self.context.current_row in self.context.errors.keys():
                # LMDTODO: This is an O(n) operation.  If instead the fact of the row having an error was part of the
                # row data, this would be O(1).  We should probably do that instead, and definitely if any row metadata
                # is on the row besides the original row number.  The thing in the special field in row could be an obj.
                continue    # Only trap the first error per row
            # NOw that we know the row number run the step and handle exceptions.
            try:
                # LMDTODO: pass a deepcopy of row
                new_data.append(step(row, context=self.context))
            except Exception as exc:
                self.process_exception(exc, step, row)
                if not isinstance(exc, DropRowException):
                    new_data.append(row)  # If we are continuing, keep the row in the data unchanged unless it's a
                    # DropRowException. (If the caller wants to change the row and also throw an exception, they can't)
        self.row_data = new_data

    def process_exception(self, exc, step, row):
        if isinstance(exc, DropRowException):
            self.context.add_dropped_row(step, row, exc.message)
        elif isinstance(exc, WarningException):
            self.context.add_warning(step, row, exc.message)
        else:
            e_name = exc.__class__.__name__
            e_message = str(exc)
            message = f"{e_name} raised ({e_message})" if e_message else f"{e_name} raised."
            logger.debug(f"Unknown exception handled in executing steps ({message}")

            match self.default_error_policy:
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
                    raise PipelineErrorException(f"Unknown error policy '{self.default_error_policy}'") from exc

    def execute_batch_step(self, step):
        self.context.current_row = 'batch'
        try:
            new_row_values = step(self.row_data, context=self.context)
            row_size_diff = len(self.row_data) - len(new_row_values)
            if row_size_diff > 0:
                self.context.add_warning(step, None, f"{row_size_diff} rows were dropped by step")
            elif row_size_diff < 0:
                self.context.add_warning(step, None, f"{abs(row_size_diff)} rows were ADDED by step")
            self.row_data = [row for row in new_row_values]
        except Exception as exc:
            self.process_exception(exc, step, None)


# LMDTODO: add a test that makes sure that a batch step followed by a row step works fine