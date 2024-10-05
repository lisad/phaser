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
from phaser.table_diff import IndexedTableDiffer

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

        pipeline_instance = Pipeline(working_dir, source_file)
        pipeline_instance.setup_phases()
        phases = pipeline_instance.phase_instances
        if len(phases) > 1:
            compare_prev = source
            prev_name = "source"
            all_column_renames = {}
            for phase in phases:
                output_name = pipeline_instance.phase_save_filename(phase)
                output = Pipeline.load(working_dir / output_name)
                phase_column_renames = get_real_renamed_columns(phase, compare_prev[0].keys(), output[0].keys())
                build_full_pipeline_rename_map(phase_column_renames, all_column_renames)
                diff_filename = working_dir / f"diff_to_{output_name}.html"
                print(f"Diff of {prev_name} and {output_name} will be saved in {diff_filename}")
                differ = IndexedTableDiffer(compare_prev, output, column_renames=phase_column_renames)
                with open(diff_filename, 'w') as diff_file:
                    diff_file.write(differ.html())
                print_summary(differ)
                prev_name = output_name
                compare_prev = Pipeline.load(working_dir / output_name) # Reload to start read from beginning
            last_one = compare_prev
        else:
            last_one = Pipeline.load(working_dir / pipeline_instance.phase_save_filename(phases[0]))
            all_column_renames = get_real_renamed_columns(phases[0], source[0].keys(), last_one[0].keys())

        # Full pipeline diff
        source = Pipeline.load(source_file)  # Reload to read file from beginning
        diff_filename = working_dir / f"diff_pipeline.html"
        differ = IndexedTableDiffer(source, last_one, column_renames=all_column_renames)
        print(f"Entire pipeline changes in {diff_filename}")
        with open(diff_filename, 'w') as diff_file:
            diff_file.write(differ.html())
        print_summary(differ)
        # TODO: create a wrapper HTML file that allows the user to navigate between the different diffs when the
        # wrapper is loaded automatically into the browser after the command runs?


def build_full_pipeline_rename_map(phase_column_renames, all_column_renames):
    for old_name, new_name in phase_column_renames.items():
        if old_name in all_column_renames.values():
            reverse_rename_lookup = {v: k for k, v in all_column_renames.items()}
            old_old_name = reverse_rename_lookup[old_name]
            all_column_renames[old_old_name] = new_name
        else:
            all_column_renames[old_name] = new_name


def get_real_renamed_columns(phase, old_column_names, new_column_names):
    # We want to know which columns were likely to have been renamed.  From the phase we get all possible
    # mappings - but some mappings weren't used (didn't appear in the source file) or were used then deleted
    # (don't appear in the destination file)
    all_possible_column_renames = phase.column_rename_dict()
    actual_column_renames = {}
    for old, new in all_possible_column_renames.items():
        if old in old_column_names and new in new_column_names:
            actual_column_renames[old] = new
    return actual_column_renames


def print_summary(differ):
    print(f"    {differ.counters['added']} rows added")
    print(f"    {differ.counters['removed']} rows removed")
    print(f"    {differ.counters['changed']} rows changed")
    print(f"    {differ.counters['unchanged']} rows unchanged")
