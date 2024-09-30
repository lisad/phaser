"""
Generate diffs from phaser sources, checkpoints, and final output

For example, given the 'weather' pipeline and the following checkpoints in a working directory, where
Validator is executed before Transformer in the pipeline:

working-dir/
    weather-240619-201208/
        source_copy.csv
        Transformer_output_temps.csv
        Validator_output_temps.csv

to generate 3 diffs from this output:

    python -m phaser diff weather working_dir


"""
from importlib import import_module
from inspect import getmembers, getmodule
from pathlib import Path

import phaser
from phaser.cli import Command
from phaser.table_diff import Differ

class DiffCommand(Command):
    def __init__(self):
        super().__init__()
        self.pipeline = None

    def add_arguments(self, parser):
        parser.add_argument("pipeline_name",
            help="pipeline with job result directory in working dir, to build diffs for")
        parser.add_argument("working_dir", help="directory with pipeline results files")

    def execute(self, args):
        args = vars(args)
        pipeline_name = args['pipeline_name']
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

        working_dir = Path(args['working_dir'])
        source_file = working_dir / Pipeline.source_copy_filename()
        source = Pipeline.load(source_file)

        old_columns = source[0].keys()

        pipeline_instance = Pipeline(working_dir, source_file)

        outputs = pipeline_instance.expected_main_outputs()
        if len(outputs) > 1:
            compare_prev = source
            for output_name in pipeline_instance.expected_main_outputs():
                output = Pipeline.load(working_dir / output_name)
                filename = working_dir / f"diff_to_{output_name}.html"
                with open(filename, 'w') as diff_file:
                    diff_file.write(Differ(compare_prev, output).html())
                compare_prev = Pipeline.load(working_dir / output_name) # Reload to start read from beginning
            last_one = compare_prev
        else:
            last_one = Pipeline.load(working_dir / outputs[0])

        # Full pipeline diff
        source = Pipeline.load(source_file)  # Reload to read file from beginning
        filename = working_dir / f"diff_pipeline.html"
        with open(filename, 'w') as diff_file:
            diff_file.write(Differ(source, last_one).html())
