import pandas as pd
import logging
from .column import make_strict_name
from .pipeline import Pipeline, Context, DropRowException
from .steps import ROW_STEP, BATCH_STEP, DATAFRAME_STEP, PROBE_VALUE

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

        self.row_data = {}  # The row dict is indexed by ORIGINAL row number.  Rows can be dropped as steps happen.
        self.headers = None
        self.dataframe_data = None
        self.default_error_policy = error_policy or Pipeline.ON_ERROR_COLLECT

    def run(self, source, destination):
        # Break down run into load, steps, error handling, save and delegate
        self.load(source)
        self.do_column_stuff()
        self.run_steps()
        if not self.context.has_errors():
            self.save(destination)
        self.report_errors_and_warnings()

    def report_errors_and_warnings(self):
        # LMDTODO: ok this needs to be much better. save to file, also send to stdout or something...
        for error in self.context.errors:
            print(error)
        for warning in self.context.warnings:
            print(warning)

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
        if "__phaser_row_num__" in df.columns.values.tolist():
            # LMDTODO: Check that the rows are correctly numbered or can be ordered by number and unique?
            pass
        else:
            df['__phaser_row_num__'] = df.reset_index().index
        self.dataframe_data = df
        self.row_data = {row['__phaser_row_num__']: row for row in self.dataframe_data.to_dict('records')}

    def do_column_stuff(self):
        self.rename_columns()
        for column in self.columns:
            column.check_required(self.headers)
            for row_num, row in self.row_data.items():
                self.row_data[row_num] = column.check_and_cast_value(row)

    def rename_columns(self):
        """ Renames columns: both using case and space ('_', ' ') matching to convert columns to preferred
        label format, and using a list of additional alternative names provided in each column definition.
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

        for row_num, row in self.row_data.items():
            self.row_data[row_num] = {rename_me(key): value for key, value in row.items()}
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

        self.dataframe_data = pd.DataFrame(self.row_data.values())
        # LMDTODO: Should saving row numbers be an option?
        self.dataframe_data.drop('__phaser_row_num__', axis='columns')
        self.dataframe_data.to_csv(destination,
                                   na_rep="NULL",   # LMDTODO Reconsider: this makes checkpoints more readable
                                                    # but may make final import harder
                                   )
        logger.info(f"{self.name} saved output to {destination}")

    def check_headers_consistent(self):
        for row in self.row_data.values():
            for field_name in row.keys():
                if field_name not in self.headers and field_name != '__phaser_row_num__':
                    raise Exception(f"At some point, {field_name} was added to the row_data and not declared a header")

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
        for row_num, row in self.row_data.items():
            self.context.current_row = row_num
            try:
                new_row = step(row, context=self.context)
            except DropRowException:
                self.row_data.popitem(row_num)
                self.context.add_warning(f"Step {step} dropped row ({row})")
        self.row_data[row_num] = new_row

    def execute_batch_step(self, step):
        new_row_values = step(list(self.row_data.values()), context=self.context)
        row_size_diff = len(self.row_data) - len(new_row_values)
        if row_size_diff > 0:
            self.context.add_warning(f"{row_size_diff} rows were dropped by step {step}")
        elif row_size_diff < 0:
            self.context.add_warning(f"{abs(row_size_diff)} rows were ADDED by step {step} ")
        self.row_data =  {row['__phaser_row_num__']: row for row in new_row_values}