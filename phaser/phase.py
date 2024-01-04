import os
from pathlib import PosixPath
import pandas as pd
from .column import make_strict_name

class Phase:
    def __init__(self, source=None, working_dir=None, dest=None, steps=None, columns=None):
        self.source = source
        self.source_filename = None
        self.working_dir = working_dir
        self.dest = dest  # Filename
        self.destination = None  # Path built from self.dest and self.working_dir
        self.steps = steps or []
        self.columns = columns or []
        self.row_data = []
        self.dataframe_data = None
        self.initialize_values()

    def initialize_values(self):
        if isinstance(self.source, str):
            self.source_filename = os.path.basename(self.source)
        elif isinstance(self.source, PosixPath):
            self.source_filename = self.source.name
        else:
            raise ValueError("Source filename attribute 'source' is not a string or Path")

        if self.dest is None:
            # LMDTODO: Ideally this should detect if previous phases have
            # already claimed this name.   Move to save function?
            self.dest = f"Transformed-{self.source_filename}"

        if not os.path.exists(self.working_dir):
            raise ValueError(f"Working dir {self.working_dir} does not exist.")

        self.destination = os.path.join(self.working_dir, self.dest)
        if str(self.destination) == str(self.source):
            raise ValueError("Destination file cannot be same as source, it will overwrite")

    def run(self):
        # Break down run into load, steps, error handling, save and delegate
        self.load()
        self.do_column_stuff()
        self.run_steps()
        self.save()

    def load(self):
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
        self.dataframe_data = pd.read_csv(self.source,
                                          dtype='str',
                                          sep=',',
                                          skip_blank_lines=True,
                                          index_col=False,
                                          comment='#')
        self.row_data = self.dataframe_data.to_dict('records')

    def headers(self):
        # LMDTODO: This is a temporary solution what would be better?  For one thing
        # this errors if the datatable is empty and the file only came with headers which is actually legit
        return self.row_data[0].keys()

    def do_column_stuff(self):
        self.rename_columns()
        for column in self.columns:
            column.check(self.headers, self.row_data)

    def rename_columns(self):
        """ Renames columns: both using case and space ('_', ' ') matching to convert columns to preferred
        label format, and using a list of additional alternative names provided in each column definition.  """
        rename_list = {alt: col.name for col in self.columns for alt in col.rename }
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

    def save(self):
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
        self.dataframe_data.to_csv(self.destination,
                                   na_rep="NULL",   # LMDTODO Reconsider: this makes checkpoints more readable
                                                    # but may make final import harder
                                   )

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
