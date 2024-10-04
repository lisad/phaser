from difflib import SequenceMatcher

"""
This table-based diff tool is specific to Phaser

 * It extracts row numbers identified with the PHASER_ROW_NUM field name, to see which rows
   have been added or removed.
 * It checks with the Phases in the Pipeline to see which columns might have been renamed.
   When iterating through rows of the changed data, it accounts for the column rename so that
   users can see which actual values have been changed ot not.
   TODO - this will require updating the CLI diff code also, to pass in the phase that is being
   run to cause the diff, to get the column renames.
   EVEN BETTER, keep the table diffing independent of phaser - when setup, the differ could be passed the 
   column-name-changes to display at top and then factor out of the field-by-field changes.
 * It uses HTML to display the changes in a table that shows added and removed rows, and added and removed columns
   TODO - display new columns with formatting, deleted columns with formatting 
 * Wraps this all up in linked HTML pages so that the entire pipeline diff can used to navigate to the
   diffs from each phase.  (TODO)
 
"""


class IndexedTableDiffer:
    def __init__(self, f1, f2, index_column_name='__phaser_row_num__', pipeline=None):
        def _no_row_num(line):
            line.pop(index_column_name)
            return line

        if isinstance(f1, dict):
            # Passing in a dict instead of a file allows for easier testing.
            self.f1_dict = f1
        else:
            self.f1_dict = {line[index_column_name]: _no_row_num(line) for line in f1}
        if isinstance(f2, dict):
            self.f2_dict = f2
        else:
            self.f2_dict = {line[index_column_name]: _no_row_num(line) for line in f2}

        row1 = list(self.f1_dict.values())[0]  # sample row from f1 to get keys which are field names
        row2 = list(self.f2_dict.values())[0]  # sample row from f2
        self.pipeline = pipeline  # LMDTODO: Instead of building pipeline logic in, wrap a generic table differ in something pipeline aware
        self.all_field_names = self._setup_column_header(row1.keys(), row2.keys())
        self.all_row_nums = sorted(set().union(self.f1_dict.keys(), self.f2_dict.keys()))
        self.formatter = None
        self.counters = {'added': 0, 'removed': 0, 'changed': 0, 'unchanged': 0}

    def _setup_column_header(self, headers1, headers2):
        column_headers = list(headers1)
        for column_name in headers2:
            if column_name not in column_headers:
                column_headers.append(column_name)
        return column_headers

    def html(self):
        return self.output(HtmlTableFormat)

    def output(self, formatter_class):
        self.formatter = formatter_class(self.all_field_names)
        self.iterate_rows()
        return self.formatter.finish()

    def iterate_rows(self):
        for row_num in self.all_row_nums:
            row1 = self.f1_dict.get(row_num)
            row2 = self.f2_dict.get(row_num)
            if row1 and row2:
                self.diff_row(row_num, row1, row2)
            elif row1 and not row2:
                self.deleted_row(row_num, row1)
            elif row2 and not row1:
                self.added_row(row_num, row2)
            else:
                raise Exception("Logic error iterating through rows in diff")

    def added_row(self, row_num, row):
        self.counters['added'] += 1
        cells = []
        for field in self.all_field_names:
            if field in row.keys():
                cells.append(self.formatter.added_text(row[field]))
            else:
                cells.append("")
        self.formatter.new_added_row(row_num, cells)

    def deleted_row(self, row_num, row):
        self.counters['removed'] += 1
        cells = []
        for field in self.all_field_names:
            if field in row.keys():
                cells.append(self.formatter.removed_text(row[field]))
            else:
                cells.append("-")
        self.formatter.new_deleted_row(row_num, cells)

    def diff_row(self, row_num, l1, l2):
        cells = []
        if all([l1.get(field) == l2.get(field) for field in self.all_field_names]):
            self.counters['unchanged'] += 1
            for field in self.all_field_names:
                cells.append(l1.get(field))
            self.formatter.new_same_row(row_num, cells)
            return

        self.counters['changed'] += 1
        for field in self.all_field_names:
            value1 = l1.get(field)
            value2 = l2.get(field)
            cells.append(self.diff_field(value1, value2))
        self.formatter.new_changed_row(row_num, cells)

    def diff_field(self, value1, value2):
        if value1 and not value2:
            return self.formatter.removed_text(value1)
        elif value2 and not value1:
            return self.formatter.added_text(value2)
        elif value1 and value2:
            diff_matcher = SequenceMatcher(None, value1, value2)
            return self.formatter.show_changes(diff_matcher.get_opcodes(), value1, value2)
        else:
            return self.formatter.NO_CHANGE_CELL_TEXT


class HtmlTableFormat:

    NO_CHANGE_CELL_TEXT = '-'  # Shown when field has no value in both old and new table
    TH_STYLE = "'text-transform: uppercase;padding:8px;border-bottom: 1px solid #e8e8e8;font-size: 0.8125rem;'"

    def __init__(self, all_field_names):
        self.all_field_names = all_field_names
        self.content = "<table style='font-family: Arial;'>"
        self.content += "<tr>" + self.header_cell("<!--change type-->") + self.header_cell("Row number")
        self.content += ''.join([self.header_cell(field) for field in self.all_field_names])
        self.content += "</tr>"

    def header_cell(self, text):
        return "<th style=" + self.TH_STYLE + ">" + text + "</th>"

    def added_text(self, text):
        return "<span style=\"color: green\">" + text + "</span>"

    def removed_text(self, text):
        return "<span style=\"color: red\">" + text + "</span>"

    def show_changes(self, op_codes, value1, value2):
        text = ""
        for op in op_codes:
            op_type, old_start, old_end, new_start, new_end = op[0], op[1], op[2], op[3], op[4]
            if op_type == 'equal':
                text += value1[old_start:old_end]
            elif op_type == 'insert':
                text += self.added_text(value2[new_start:new_end])
            elif op_type == 'replace':
                text += self.removed_text(value1[old_start:old_end])
                text += self.added_text(value2[new_start:new_end])
            elif op_type == 'delete':
                text += self.removed_text(value1[old_start:old_end])
            else:
                raise Exception("Table differ does not handle unknown op type: " + op_type)
        return text

    def new_added_row(self, row_num, cells):
        cells.insert(0, "Added")
        self.new_row(row_num, cells)

    def new_deleted_row(self, row_num, cells):
        cells.insert(0, "Deleted")
        self.new_row(row_num, cells)

    def new_same_row(self, row_num, cells):
        cells.insert(0, "Same")
        self.new_row(row_num, cells)

    def new_changed_row(self, row_num, cells):
        cells.insert(0, "Changed")
        self.new_row(row_num, cells)

    def new_row(self, row_num, cells):
        cells.insert(1, row_num)
        html_cells = ["<td style='padding:8px;'>" + str(cell) + "</td>" for cell in cells]
        row = "<tr>" + "\n".join(html_cells) + "</tr>"
        self.content += row

    def finish(self):
        return self.content + "</table>"
