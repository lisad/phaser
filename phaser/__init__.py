""" phaser - a library to simplify automated batch-oriented complex data integration pipelines

`Pipeline` : organizes a data pipeline into multiple phases, each with checkpoints so that the pipeline can be
continued from any phase.

`Phase` : Organizes the work of a complicated pipeline into a smaller unit. It still contains multiple steps and
column definitions/fixes, but it can be run standalone from its input data and have its output examined as both
a checkpoint and input to the next phase.

PipelineErrorException, DropRowException, WarningException : these exceptions can be used in custom steps to
define what should happen when the exception is raised.

`row_step`, `batch_step` : These are decorators used to wrap custom steps so they can be called by a Phase with the
right input data, and check the output and handle exceptions.

check_unique, sort_by : Built-in steps that can be declared to operate on a column, and included in the steps of
a Phase.

Column, IntColumn, DateColumn, DateTimeColumn : Define column instances by name and type, and pass them to the Phase to
have datatype casting and name canonicalization done automatically.

"""

# Note: not sure this documentation is going to stay here.   There is  advice to put module documentation in the
# module's __init__.py file [here](https://realpython.com/documenting-python-code/)
# but that may be more relevant for smaller modules.  We should also consider readthedocs.io but that's a big
# commitment to build for that.

# Set default logging handler to avoid "No handler found" warnings.
import logging
from logging import NullHandler

from phaser.pipeline import Pipeline
from phaser.context import Context
from phaser.constants import PHASER_ROW_NUM, ON_ERROR_WARN, ON_ERROR_COLLECT, ON_ERROR_DROP_ROW, ON_ERROR_STOP_NOW
from phaser.exceptions import PhaserError, DataErrorException, DataException, DropRowException, WarningException
from phaser.phase import Phase
from phaser.steps import row_step, batch_step, dataframe_step, context_step
from phaser.builtin_steps import check_unique, sort_by, filter_rows
from phaser.column import Column, IntColumn, DateColumn, DateTimeColumn, FloatColumn, BooleanColumn
from phaser.io import read_csv, save_csv, ExtraMapping, ExtraRecords
from phaser.table_diff import HtmlTableFormat, FormatterBase, IndexedTableDiffer

logging.getLogger(__name__).addHandler(NullHandler())

__version__ = 0.3
