
class DataException(Exception):
    """ DataException subclasses are thrown when processing data, to trigger the phaser library code to follow
     error-handling policy, often with respect to the row the issue occurs in."""

    def __init__(self, message, **kwargs):
        self.message = message
        self.row = kwargs.pop('row') if 'row' in kwargs else None


    def __str__(self):
        return self.message


class DataErrorException(DataException):
    """ Using this exception will cause the data or the specific row to be listed among errors.
    If possible, the pipeline will keep going to the end of a phase, collecting more errors, so they can all
    be dealt with.  """
    pass


class DropRowException(DataException):
    """ Throwing this exception in a row_step will cause the current row to be dropped. Rows dropped this
    way will be listed in the phase results report along with a reason given in the exception constructor. """
    pass


class WarningException(DataException):
    """ Throwing this exception will add warnings to the output of the pipeline.  While it can't be used
    in methods where a return value is needed, it can be used in methods that check results without returning
    fixed data.  """
    pass


class PhaserError(Exception):
    """ PhaserError indicates not a data issue to handle in processing, but a coding error in phaser
    or in client code that does not meet the interface contract.  Example: in order to avoid
    accidentally dropping rows, a row step must return a row or throw an exception.  If a row
    step returns none, the phaser library raises PhaserError to report this to the developer."""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

