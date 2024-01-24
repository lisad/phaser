# A script that loads the Phaser environment and runs a pipeline
#
# This is the prototype from which we can build more subcommands to use for
# running various tasks as part of a data integration system.

import sys
from importlib import import_module
import inspect
import phaser

class PipelineRunner:
    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.pipeline_name = self.argv[1]
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
        self.working_dir = self.argv[2]
        self.source = self.argv[3]

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
    runner = PipelineRunner(sys.argv)
    runner.execute()

if __name__ == '__main__':
    main()
