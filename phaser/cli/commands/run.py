"""
Run a pipeline

A pipeline must be declared as a subclass of `phaser.Pipeline` inside a module
that lives in a package named `pipelines`. The name of the file containing the pipeline should be
given to this command as the first positional argument.

For example, given the following directories and files

    app/
        pipelines/
            __init__.py
            transportation.py
            weather.py

to run the "weather" pipeline, from within the 'app' directory, invoke the following with appropriate working
directory and source arguments:

    python -m phaser run weather <working_dir> <source>
"""

from importlib import import_module
from inspect import getmembers, getmodule
from pathlib import Path

import phaser
from phaser.cli import Command
from phaser.constants import *


class RunPipelineCommand(Command):

    def __init__(self):
        super().__init__()
        self.pipeline = None
        self.sources_needing_initialization = None

    def add_arguments(self, parser):
        parser.add_argument("pipeline_name", help="pipeline to run")
        parser.add_argument("working_dir", help="directory to output phase results")
        parser.add_argument("source", help="file to use as initial source")
        parser.add_argument('--error-policy',
                            help="An error policy provided here will override the pipeline's default policy.",
                            choices=[ON_ERROR_WARN, ON_ERROR_COLLECT, ON_ERROR_DROP_ROW, ON_ERROR_STOP_NOW])

    def has_incremental_arguments(self, args):
        return True

    def instantiate_pipeline(self, args):
        pipeline_name = args.pipeline_name
        # Pipelines are expected to be defined in a module in the `pipelines`
        # package. The module name is given as the command line argument, and
        # the sole subclass of phaser.Pipeline is located and invoked with
        # additional command line arguments.
        pipeline_module = import_module(f"pipelines.{pipeline_name}")

        def is_pipeline_class(m):
            # isinstance(attr, type) is the way to check that the attr is
            # a class This makes no sense to me, as I would think anything is
            # a "type", not just a thing that is a "class."
            return (isinstance(m, type) and
                    issubclass(m, phaser.Pipeline) and
                    getmodule(m) == pipeline_module)

        pipelines = getmembers(pipeline_module, is_pipeline_class)
        if len(pipelines) != 1:
            raise Exception(f"Found {len(pipelines)} Pipelines declared in module '{pipeline_module}'. Need only 1.")
        # pipelines is a tuple of names and values. We want the value which is
        # a class object.
        Pipeline = pipelines[0][1]

        verbose = args.verbose
        working_dir = Path(args.working_dir)
        source = args.source
        error_policy = args.error_policy

        self.pipeline = Pipeline(working_dir, source, verbose=verbose, error_policy=error_policy, name=pipeline_name)

    def add_incremental_arguments(self, args, parser):
        # In order to know which extra arguments the command will need, we first need to instantiate the pipeline
        self.instantiate_pipeline(args)
        self.sources_needing_initialization = self.pipeline.sources_needing_initialization()
        for source in self.sources_needing_initialization:
            parser.add_argument(f"--{source}", help=f"path to source file for {source}", required=True)

    def execute(self, args):
        # Convert a argparse.Namespace object to a dict for subscript access
        args = vars(args)
        for source in self.sources_needing_initialization:
            self.pipeline.init_source(source, args[source])

        print(f"Running pipeline '{self.pipeline.__class__.__name__}'")
        try:
            self.pipeline.run()
        except phaser.DataException as e:
            print("Error processing data.  ", e.message)
