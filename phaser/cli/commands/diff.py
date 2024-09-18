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
from difflib import SequenceMatcher

import phaser
from phaser.cli import Command
from phaser.constants import PHASER_ROW_NUM

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

class Differ:
    def __init__(self, f1, f2):
        def no_row_num(line):
            line.pop(PHASER_ROW_NUM)
            return line
        self.f1_dict = {line[PHASER_ROW_NUM]: no_row_num(line) for line in f1}
        self.f2_dict = {line[PHASER_ROW_NUM]: no_row_num(line) for line in f2}
        row1 = list(self.f1_dict.values())[0]  # sample row from f1
        row2 = list(self.f2_dict.values())[0]  # sample row from f2
        self.all_field_names = set().union(list(row1.keys()), list(row2.keys()))
        self.all_row_nums = sorted(set().union(self.f1_dict.keys(), self.f2_dict.keys()))
        self.outputter=None

    def html(self):
        self.outputter = HtmlTableOutput(self.all_field_names)
        self.iterate_rows()
        return self.outputter.finish()

    def iterate_rows(self):
        for row_num in self.all_row_nums:
            row1 = self.f1_dict.get(row_num)
            row2 = self.f2_dict.get(row_num)
            if row1 and row2:
                self.diff_row(row1, row2)
            elif row1 and not row2:
                self.deleted_row(row1)
            elif row2 and not row1:
                self.added_row(row2)
            else:
                raise Exception("Logic error iterating through rows in diff")

    def added_row(self, row):
        for field in self.all_field_names:
            if field in row.keys():
                self.outputter.add_cell(self.outputter.added_text(row[field]))
            else:
                self.outputter.add_cell("")
        self.outputter.new_row()

    def deleted_row(self, row):
        for field in self.all_field_names:
            if field in row.keys():
                self.outputter.add_cell(self.outputter.removed_text(row[field]))
            else:
                self.outputter.add_cell("-")
        self.outputter.new_row()

    def diff_row(self, l1, l2):
        for field in self.all_field_names:
            value1 = l1.get(field)
            value2 = l2.get(field)
            if value1 and not value2:
                self.outputter.add_cell(self.outputter.removed_text(value1))
            elif value2 and not value1:
                self.outputter.add_cell(self.outputter.added_text(value2))
            elif value1 and value2:
                difflib = SequenceMatcher(None, value1, value2)
                self.outputter.add_cell(self.outputter.op_codes(difflib.get_opcodes(), value1, value2))
            else:
                self.outputter.add_cell('-')
        self.outputter.new_row()

class HtmlTableOutput:
    def __init__(self, all_field_names):
        self.all_field_names = all_field_names
        self.content = "<table>"
        self.content += "<tr>" + ''.join(["<th>" + field + "</th>" for field in self.all_field_names]) + "</tr>"
        self.current_row = []

    def added_text(self, text):
        return "<span style=\"color: green\">" + text + "</span>"

    def removed_text(self, text):
        return "<span style=\"color: red\">" + text + "</span>"

    def add_cell(self, cell_content):
        self.current_row.append(str(cell_content))

    def op_codes(self, op_codes, value1, value2):
        text = ""
        for op in op_codes:
            op_type, old_start, old_end, new_start, new_end = op[0], op[1], op[2], op[3], op[4]
            if op_type == 'equal':
                text += value1[old_start:old_end]
            elif op_type == 'insert':
                text += self.added_text(value2[new_start:new_end])
            else:
                raise Exception("More to do", op_type)
        return text

    def new_row(self):
        cells = ["<td>" + item + "</td>" for item in self.current_row]
        row =  "<tr>" + "\n".join(cells) + "</tr>"
        self.content += row
        self.current_row = []

    def finish(self):
        return self.content + "</table>"