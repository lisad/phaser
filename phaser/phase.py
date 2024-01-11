import pandas as pd
import logging
from .column import make_strict_name

logger = logging.getLogger('phaser')
logger.addHandler(logging.NullHandler())


class Phase:
    # Subclasses can override to set source for all instances if appropriate
    source = None
    working_dir = None
    steps = []
    columns = []

    def __init__(self, name=None, steps=None, columns=None):
        self.name = name or self.__class__.__name__
        self.steps = steps or self.__class__.steps
        self.columns = columns or self.__class__.columns

        self.row_data = []
        self.dataframe_data = None

    def run(self, source, destination):
        # Break down run into load, steps, error handling, save and delegate
        self.load(source)
        self.do_column_stuff()
        self.run_steps()
        self.save(destination)

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
        self.dataframe_data = pd.read_csv(source,
                                          dtype='str',
                                          sep=',',
                                          skip_blank_lines=True,
                                          index_col=False,
                                          comment='#')
        self.row_data = self.dataframe_data.to_dict('records')

    def headers(self):
        # LMDTODO: This is a temporary solution what would be better?  For one thing this approach
        # errors if the datatable is empty and the file only came with headers which is actually legit
        return self.row_data[0].keys()

    def do_column_stuff(self):
        self.rename_columns()
        headers = self.headers()
        for column in self.columns:
            column.check(headers, self.row_data)

    def rename_columns(self):
        """ Renames columns: both using case and space ('_', ' ') matching to convert columns to preferred
        label format, and using a list of additional alternative names provided in each column definition.
        """
        rename_list = {alt: col.name for col in self.columns for alt in col.rename}
        strict_name_list = {make_strict_name(col.name): col.name for col in self.columns}
        new_data = []
        for row in self.row_data:
            new_row = {}
            for key, value in row.items():
                new_key = key.strip()
                if make_strict_name(new_key) in strict_name_list.keys():
                    new_key = strict_name_list[make_strict_name(new_key)]  # Convert to declared capital'n/separ'n
                if key in rename_list.keys():
                    new_key = rename_list[new_key]   # Do declared renames
                new_row[new_key] = row[key]
            new_data.append(new_row)
        self.row_data = new_data

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
        self.dataframe_data = pd.DataFrame(self.row_data)
        self.dataframe_data.to_csv(destination,
                                   na_rep="NULL",   # LMDTODO Reconsider: this makes checkpoints more readable
                                                    # but may make final import harder
                                   )
        logger.info(f"{self.name} saved output to {destination}")

    def run_steps(self):
        if self.row_data is None or self.row_data == []:
            raise Exception("No data loaded yet")
        for step in self.steps:
            step_type = step(self, "__PROBE__")  # LMDTODO Is there a better way to find out what kind of step?
            if step_type == "row_step":
                new_data = []
                for row in self.row_data:
                    new_data.append(step(self, row))
                self.row_data = new_data
            elif step_type == "batch_step":
                self.row_data = step(self, self.row_data)
            elif step_type == "dataframe_step":
                raise Exception("Not implemented yet")
            else:
                raise Exception("Unknown step type")
