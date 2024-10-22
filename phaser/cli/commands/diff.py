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
import webbrowser
import os

import phaser
from phaser.cli import Command
from phaser.column import make_strict_name
from phaser.table_diff import IndexedTableDiffer

class DiffCommand(Command):
    def __init__(self):
        super().__init__()
        self.pipeline = None
        self.working_dir = None
        self.all_column_renames = {}
        self.all_phases_diffable = True
        self.diff_files = {}

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

        self.working_dir = Path(args['working_dir'])
        self.pipeline = Pipeline(self.working_dir, Pipeline.source_copy_filename())
        self.pipeline.setup_phases()
        phases = self.pipeline.phase_instances

        # Each phase diff if appropriate
        if len(phases) > 1:
            prev_name = self.pipeline.source_copy_filename()
            for phase in phases:
                output_name = self.pipeline.phase_save_filename(phase)
                self.diff_phase(phase, prev_name)
                prev_name = output_name

        # Full pipeline diff if appropriate
        if self.all_phases_diffable:
            source_data = self.pipeline.load(self.working_dir / self.pipeline.source_copy_filename())
            end_data = self.pipeline.load(self.working_dir / self.pipeline.phase_save_filename(phases[-1]))
            if len(phases) == 1 and self.all_column_renames is None:
                # If there was only one phase, then also all_column_renames was never added to.
                self.all_column_renames = get_real_renamed_columns(phases[0], source_data[0].keys(), end_data[0].keys())

            diff_filepath = self.working_dir / "diff_pipeline.html"
            differ = IndexedTableDiffer(source_data, end_data, column_renames=self.all_column_renames)
            print(f"Entire pipeline changes in {diff_filepath}")
            with open(diff_filepath, 'w') as diff_file:
                diff_file.write(differ.html())
            print_summary(differ)
            self.diff_files["Pipeline"] = "diff_pipeline.html"

        # Create an HTML file to wrap all the others and open it (could make this an option in future):
        with open(self.working_dir / "diff_wrapper.html", 'w') as diff_wrapper_file:
            diff_wrapper_file.write(get_wrapper_html(self.diff_files, self.all_phases_diffable))
        full_path = 'file://' + str(os.path.realpath(self.working_dir / "diff_wrapper.html"))
        webbrowser.open(full_path)


    def diff_phase(self, phase, previous_output_name):
        old_file = previous_output_name
        new_file = self.pipeline.phase_save_filename(phase)
        if phase.diffable():
            old_data = self.pipeline.load(self.working_dir / old_file)
            new_data = self.pipeline.load(self.working_dir / new_file)
            phase_column_renames = get_real_renamed_columns(phase, old_data[0].keys(), new_data[0].keys())
            self.build_full_pipeline_rename_map(phase_column_renames, self.all_column_renames)
            diff_filepath = self.working_dir / f"diff_to_{new_file}.html"
            print(f"Diff of {old_file} and {new_file} will be saved in {diff_filepath}")
            differ = IndexedTableDiffer(old_data, new_data, column_renames=phase_column_renames)
            with open(diff_filepath, 'w') as diff_file:
                diff_file.write(differ.html())
            print_summary(differ)
            self.diff_files[phase.name] = f"diff_to_{new_file}.html"
        else:
            print(f"Skipping diff of {old_file} and {new_file} - phase may reorganize data")
            self.all_phases_diffable = False

    def build_full_pipeline_rename_map(self, phase_column_renames, all_column_renames):
        for old_name, new_name in phase_column_renames.items():
            if old_name in all_column_renames.values():
                reverse_rename_lookup = {v: k for k, v in all_column_renames.items()}
                old_old_name = reverse_rename_lookup[old_name]
                all_column_renames[old_old_name] = new_name
            else:
                all_column_renames[old_name] = new_name


def get_wrapper_html(diff_filenames, all_phases_included):
    buttons = ""
    for phase_name, diff_filename in diff_filenames.items():
        buttons += f"<input type='button' onclick='load_diff_file(\"{diff_filename}\");' value=\"{phase_name}\" />"
    if all_phases_included:
        if len(diff_filenames) == 1:
            intro = "Diff of changes in single-phase pipeline:"
        else:
            intro = "Diffs of changes by each phase, and entire pipeline:"
    else:
        intro = "Diffs of any phases that are diff-able (did not reorganize data):"

    return """
        <html><head>
        <style type="text/css">
            input { margin: 12px; padding: 4px;}
            p { font-family: Arial; padding: 20px; }
        </style>
        </head>
        <body>
            <div id='nav'>
                <p> """ + intro + buttons + """
                </p>
            </div>
            <div id='display'></div>
        <script>
            function load_diff_file(file_name) {
                document.getElementById("display").innerHTML =
                    '<embed type="text/html" src="' + file_name + '" width="100%" height="800" >';
            }
        </script>
        </body></html>
    """


def get_real_renamed_columns(phase, old_column_names, new_column_names):
    # We want to know which columns were likely to have been renamed.  From the phase we get all possible
    # mappings - but some mappings weren't used (didn't appear in the source file) or were used then deleted
    # (don't appear in the destination file)
    all_explicit_column_renames = phase.column_rename_dict()
    actual_column_renames = {}
    for old, new in all_explicit_column_renames.items():
        # If the old name didn't appear in the input, the rename didn't ACTUALLY happen
        if old in old_column_names and new in new_column_names:
            actual_column_renames[old] = new
    # Along with the renames, there are also columns that change in capitalization/underscore to the programmer's
    # preferred variant
    strict_new_names = {make_strict_name(name): name for name in new_column_names}
    for old in old_column_names:
        strict_old_name = make_strict_name(old)
        if strict_old_name in strict_new_names.keys() and strict_new_names[strict_old_name] != old:
            actual_column_renames[old] = strict_new_names[strict_old_name]

    return actual_column_renames


def print_summary(differ):
    print(f"    {differ.counters['added']} rows added")
    print(f"    {differ.counters['removed']} rows removed")
    print(f"    {differ.counters['changed']} rows changed")
    print(f"    {differ.counters['unchanged']} rows unchanged")
