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

import phaser
from phaser.cli import Command

class RunPipelineCommand(Command):
    def add_arguments(self, parser):
        parser.add_argument("pipeline_name", help="pipeline to run")
        parser.add_argument("working_dir", help="directory to output phase results")
        parser.add_argument("source", help="file to use as initial source")

# TODO: figure out how to get additionaal needed sources from a pipeline and add
# them as required arguments

    def execute(self, args):
        pipeline_name = args.pipeline_name
        # Pipelines are expected to be defined in a module in the `pipelines`
        # package. The module name is given as the command line argument, and
        # the sole subclass of phaser.Pipeline is located and invoked with
        # additional command line arguments.
        pipeline_module = import_module(f"pipelines.{pipeline_name}")
        verbose = False
        try:
            verbose = args.verbose
        except:
            pass

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
        working_dir = args.working_dir
        source = args.source

        print(f"Running pipeline '{Pipeline.__name__}'")
        pipeline = Pipeline(working_dir, source, verbose=verbose)
        pipeline.run()
