from difflib import SequenceMatcher
from abc import ABC, abstractmethod

"""
This table-based diff tool and HTML formatter are inspired by functionality in django import-export.  Unlike
text-oriented diffing tools, it operates field by field to identify which fields in a table changed from one
version to another.  For best results:
 * Rows should be numbered in both files, such that removed, added and reordered rows can be detected and shown
 * Column names that were changed from one table to another may be passed in so that those don't look like
   a deleted column and an added column
   
To use this tool, pass in tables in the form of lists of dicts.  Each row is indexed with the column name.
Another formatter besides the HTML formatter included here may be passed into the output method.
 
"""


class IndexedTableDiffer:
    def __init__(self, f1, f2, index_column_name='__phaser_row_num__', column_renames=dict()):
        """
        Constructs the differ that will find out what was added/removed in f2, a table, relative to f1

        :param f1: a table formatted record-oriented (list of dicts)
        :param f2: a table formatted the same way
        :param index_column_name: the name of the row number field in each dict
        :param column_renames: a dict with mappings from old to new column names if known
        """
        def _no_row_num(line):
            line.pop(index_column_name)
            return line

        if isinstance(f1, dict):
            # Passing in a dict by row number instead of a list of rows allows for easier testing.
            self.f1_dict = f1
        else:
            self.f1_dict = {line[index_column_name]: _no_row_num(line) for line in f1}
        if isinstance(f2, dict):
            self.f2_dict = f2
        else:
            self.f2_dict = {line[index_column_name]: _no_row_num(line) for line in f2}

        row1 = list(self.f1_dict.values())[0]  # sample row from f1 to get keys which are field names
        row2 = list(self.f2_dict.values())[0]  # sample row from f2

        self.old_and_new_columns = self.merge_column_headers(row1.keys(), row2.keys(), column_renames)

        self.all_row_nums = sorted(set().union(self.f1_dict.keys(), self.f2_dict.keys()))
        self.formatter = None
        self.counters = {'added': 0, 'removed': 0, 'changed': 0, 'unchanged': 0}

    def merge_column_headers(self, headers1, headers2, column_renames):
        for old, new in column_renames.items():
            if old not in headers1 or new not in headers2:
                raise Exception("Column_renames should be a dict with keys from file1 columns mapping to file2 " +
                                f"columns. Mapping '{old}' to '{new}' not found.")

        column_headers = list(headers1)
        for column_name_from_2 in headers2:
            if column_name_from_2 not in column_headers and column_name_from_2 not in column_renames.values():
                # Column names that are completely new in file2 show at the end of the diff column headers.
                column_headers.append(column_name_from_2)
        old_and_new_columns = [(item, column_renames.get(item, item)) for item in column_headers]
        return old_and_new_columns

    def html(self):
        """
        Returns the difference between two tables formatted in the default format, HTML
        :return: string
        """
        return self.output(HtmlTableFormat)

    def output(self, formatter_class):
        """
        Returns the difference between two tables formatted in a custom class.
        :param formatter_class: a child of FormatterBase
        :return: string
        """
        self.formatter = formatter_class(self.old_and_new_columns)
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
        for old_name, new_name in self.old_and_new_columns:
            if new_name in row.keys():
                cells.append(self.formatter.added_text(row[new_name]))
            else:
                cells.append("")
        self.formatter.new_added_row(row_num, cells)

    def deleted_row(self, row_num, row):
        self.counters['removed'] += 1
        cells = []
        for old_name, new_name in self.old_and_new_columns:
            if old_name in row.keys():
                cells.append(row[old_name])
            else:
                cells.append("")
        self.formatter.new_deleted_row(row_num, cells)

    def diff_row(self, row_num, l1, l2):
        cells = []
        if all([l1.get(old_name) == l2.get(new_name) for (old_name, new_name) in self.old_and_new_columns]):
            self.counters['unchanged'] += 1
            for old_name, new_name in self.old_and_new_columns:
                cells.append(l1.get(old_name))
            self.formatter.new_same_row(row_num, cells)
            return

        self.counters['changed'] += 1
        for old_name, new_name in self.old_and_new_columns:
            cells.append(self.diff_field(l1.get(old_name), l2.get(new_name)))
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


class FormatterBase(ABC):
    @abstractmethod
    def added_text(self, text):
        """
        Formats text that is new (e.g. green, underlined, with a '+' sign as appropriate for the style)
        :param text: The text that is new
        :return: string
        """
        return text

    @abstractmethod
    def removed_text(self, text):
        """
        Formats text that is removed (e.g. red, strike-through, with a '-' sign as appropriate for the style)
        :param text: The text that is new
        :return: string
        """
        return text

    @abstractmethod
    def show_changes(self, op_codes, value1, value2):
        """
        Formats a series of changes between two strings, using op_codes and offsets generated by python's
        SequenceMatcher
        :param op_codes: a list of tuples, where each tuple has an op_code and four offset values
        :param value1: The old value that was compared with SequenceMatcher
        :param value2: THe new value compared with SequenceMatcher
        :return: string
        """
        pass

    @abstractmethod
    def new_added_row(self, row_num, cells):
        """
        Adds a row to the formatter's content, that was added in the 2nd table compared.
        :param row_num: int
        :param cells: list of cells (same length as list of old and new columns)
        :return: None
        """
        pass

    @abstractmethod
    def new_deleted_row(self, row_num, cells):
        """
        Adds a row to the formatter's content, that was deleted in the 2nd table compared.
        :param row_num: int
        :param cells: list of cells (same length as list of old and new columns)
        :return: None
        """
        pass

    @abstractmethod
    def new_same_row(self, row_num, cells):
        """
        Adds a row to the formatter's content, that was unchanged between the two tables.
        :param row_num: int
        :param cells: list of cells (same length as list of old and new columns)
        :return: None
        """
        pass

    @abstractmethod
    def new_changed_row(self, row_num, cells):
        """
        Adds a row to the formatter's content, that was changed between the first and second tables.
        :param row_num: int
        :param cells: list of cells (same length as list of old and new columns)
        :return: None
        """
        pass

    @abstractmethod
    def finish(self):
        """
        Finishes the formatting work (e.g. closing markup) and returns the result
        :return: string
        """
        pass


class HtmlTableFormat(FormatterBase):

    NO_CHANGE_CELL_TEXT = '-'  # Shown when field has no value in both old and new table
    STYLESHEET = """
        <style type="text/css">
            table { font-family: Arial; padding: 20px; }
            table td { text-align: end; padding:8px; }
            table th { 
                text-transform: uppercase;
                padding:8px;
                border-bottom: 1px solid #e8e8e8;
                font-size: 0.8125rem; 
            }
            .newtext { color: green; text-decoration: underline }
            .deltext { color: red; text-decoration: line-through }
        </style>
    """

    def __init__(self, old_and_new_columns):
        self.content = self.STYLESHEET
        self.content += "<table>"
        self.content += self.header_row(old_and_new_columns)

    def header_row(self, old_and_new_columns):
        cells = ["<!--change type-->", "Row number"]
        for old_name, new_name in old_and_new_columns:
            if old_name == new_name:
                cells.append(old_name)
            else:
                cells.append(self.removed_text(old_name) + "<br/>" + self.added_text(new_name))
        return "<tr>" + ''.join([f"<th>{cell}</th>" for cell in cells]) + "</tr>"

    def added_text(self, text):
        return "<span class=\"newtext\">" + text + "</span>"

    def removed_text(self, text):
        return "<span class=\"deltext\">" + text + "</span>"

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
        cells.insert(0, "<i>Added</i>")
        self.new_row(row_num, cells)

    def new_deleted_row(self, row_num, cells):
        cells.insert(0, "<i>Deleted</i>")
        cells = [f"<span style=\"color: grey\">{value}</span>" for value in cells]
        self.new_row(row_num, cells)

    def new_same_row(self, row_num, cells):
        cells.insert(0, "<i>Same</i>")
        self.new_row(row_num, cells)

    def new_changed_row(self, row_num, cells):
        cells.insert(0, "<i>Changed</i>")
        self.new_row(row_num, cells)

    def new_row(self, row_num, cells):
        cells.insert(1, row_num)
        html_cells = ["<td>" + str(cell) + "</td>" for cell in cells]
        row = "<tr>" + "\n".join(html_cells) + "</tr>"
        self.content += row

    def finish(self):
        return self.content + "</table>"
