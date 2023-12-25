import os
from pathlib import PosixPath
import pandas as pd

class Phase:
    # LMDTODO How to make Phase abstract base class

    def __init__(self, source=None, working_dir=None, dest=None, steps=None):
        self.source = source
        self.working_dir = working_dir
        self.dest = dest
        self.steps = steps or []
        self.data = []
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
        self.data = self.load()
        self.run_steps()
        self.save()

    def load(self):
        # LMDTODO: Read all the read_csv options and choose the right ones and document
        self.dataframe_data = pd.read_csv(self.source)
        self.row_data = self.dataframe_data.to_dict('records')
        print(self.row_data)

    def save(self):
        # LMDTODO: Synch the dataframe version every step rather than just on save
        self.dataframe_data = pd.DataFrame(self.row_data)
        # LMDTODO: Read all the to_csv options and choose the right ones haha
        self.dataframe_data.to_csv(self.destination)

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
            elif step_type == "dataframe_step":
                raise Exception("Not implemented yet")
            else:
                raise Exception("Unknown step type")