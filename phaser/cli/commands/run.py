"""
Run a pipeline

A pipeline must be declared as a subclass of `phaser.Pipeline` inside a module
that lives in a package named `pipelines`. The name of the module should be
given to this command as the first positional argument.

For example, given the following directories and files:

    app/
        pipelines/
            __init__.py
            transportation.py
            weather.py

to run a pipeline, invoke

    python -m phaser run weather <working_dir> <source>
"""

from importlib import import_module
from inspect import getmembers, getmodule

import phaser
from phaser.cli import Command

class RunPipelineCommand(Command):
    def __is_pipeline_class(obj):
        # isinstance(attr, type) is the way to check that the attr is a class
        # This makes no sense to me, as I would think anything is a "type", not
        # just a thing that is a "class."
        return isinstance(obj, type) and issubclass(obj, phaser.Pipeline)

    def add_arguments(self, parser):
        parser.add_argument("pipeline_name", help="pipeline to run")
        parser.add_argument("working_dir", help="directory to output phase results")
        parser.add_argument("source", help="file to use as initial source")

    def execute(self, args):
        self.pipeline_name = args.pipeline_name
        # Pipelines are expected to be defined in a module in the `pipelines`
        # package. The module name is given as the command line argument, and
        # the sole subclass of phaser.Pipeline is located and invoked with
        # additional command line arguments.
        pipeline_module = import_module(f"pipelines.{self.pipeline_name}")
        pipelines = [
            p for p
            in getmembers(pipeline_module, RunPipelineCommand.__is_pipeline_class)
            if getmodule(p[1]) == pipeline_module
            ]
        if len(pipelines) != 1:
            raise Exception(f"Found {len(pipelines)} Pipelines declared in module '{pipeline_module}'. Need only 1.")
        # pipelines is a tuple of names and values. We want the value which is
        # a class object.
        self.Pipeline = pipelines[0][1]
        self.working_dir = args.working_dir
        self.source = args.source

        print(f"Running pipeline '{self.Pipeline.__name__}'")
        pipeline = self.Pipeline(self.working_dir, self.source)
        pipeline.run()
