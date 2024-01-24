# A script that loads the Phaser environment and runs a pipeline
#
# This is the prototype from which we can build more subcommands to use for
# running various tasks as part of a data integration system.

import argparse
from importlib import import_module
import inspect
import phaser

class PipelineRunner:
    def cmd_name():
        return "run"

    def cmd_help():
        return "run a pipeline"

    def add_arguments(parser):
        parser.add_argument("pipeline_name", help="pipeline to run")
        parser.add_argument("working_dir", help="directory to output phase results")
        parser.add_argument("source", help="file to use as initial source")

    def __init__(self, args):
        self.pipeline_name = args.pipeline_name
        # Pipelines are expected to be defined in a module in the `pipelines`
        # package. The module name is given as the command line argument, and
        # the sole subclass of phaser.Pipeline is located and invoked with
        # additional command line arguments.
        pipeline_module = import_module(f"pipelines.{self.pipeline_name}")
        pipelines = [
            p for p
            in inspect.getmembers(pipeline_module, PipelineRunner.__is_pipeline_class)
            if inspect.getmodule(p[1]) == pipeline_module
            ]
        if len(pipelines) != 1:
            raise Exception(f"Found {len(pipelines)} Pipelines declared in module '{pipeline_module}'. Need only 1.")
        # pipelines is a tuple of names and values. We want the value which is
        # a class object.
        self.Pipeline = pipelines[0][1]
        self.working_dir = args.working_dir
        self.source = args.source

    def __is_pipeline_class(obj):
        # isinstance(attr, type) is the way to check that the attr is a class
        # This makes no sense to me, as I would think anything is a "type", not
        # just a thing that is a "class."
        return isinstance(obj, type) and issubclass(obj, phaser.Pipeline)

    def execute(self):
        print(f"Running pipeline '{self.Pipeline.__name__}'")
        pipeline = self.Pipeline(self.working_dir, self.source)
        pipeline.run()

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    commands = [ PipelineRunner ]
    for command in commands:
        subparser = subparsers.add_parser(command.cmd_name(), help=command.cmd_help())
        command.add_arguments(subparser)
    args = parser.parse_args()

    runner = PipelineRunner(args)
    runner.execute()

if __name__ == '__main__':
    main()
