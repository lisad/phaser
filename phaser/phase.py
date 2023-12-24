import os
from pathlib import PosixPath
import pandas as pd

class Phase:
    # LMDTODO How to make Phase abstract base class

    def __init__(self, source=None, working_dir=None):
        self.source = source or self.__class__.source
        self.working_dir = working_dir or self.__class__.working_dir
        self.initialize_values()

    def initialize_values(self):
        if isinstance(self.source, str):
            self.source_filename = os.path.basename(self.source)
        elif isinstance(self.source, PosixPath):
            self.source_filename = self.source.name
        else:
            raise ValueError("Source filename attribute 'source' is not a string or Path")

        if not os.path.exists(self.working_dir):
            raise ValueError(f"Working dir {self.working_dir} does not exist.")
        destination_filename = '-'.join([self.__class__.__name__, self.source_filename])
        self.destination = os.path.join(self.working_dir, destination_filename)

    def run(self):
        # Break down run into load, steps, error handling, save and delegate
        self.data = self.load()
        self.save()

    def load(self):
        return pd.read_csv(self.source)


    def save(self):
        self.data.to_csv(self.destination)