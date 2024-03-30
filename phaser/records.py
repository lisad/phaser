from collections import UserDict, UserList
from functools import cached_property
from .exceptions import PhaserError


# Defined twice to avoid circular import - if we keep this overall approach without further refactoring
# that makes this moot, we can fix this later
PHASER_ROW_NUM = '__phaser_row_num__'


def row_num_generator():
    value = 1
    while True:
        yield value
        value += 1


class Records(UserList):
    """ Records holds the records or rows passed to phases, together with row numbers (indexed from 1) """
    def __init__(self, *args, **kwargs):
        """
        >>> str(Records([{'id': 18, 'val': 'a'}]))
        "[(row_num=1, data={'id': 18, 'val': 'a'})]"
        >>> Records()
        []
        """
        row_num_gen = kwargs.get('row_num_generator', row_num_generator())
        super().__init__(args[0] if args else None)
        # Slicing a UserList results in constructing a brand new list, which
        # would reset the row_num for our records if we were to recreated them
        # from scratch. But if the elements of the incoming list are already
        # `PhaseRecord`s, then just leave them alone.
        # This is also generally helpful in steps where the record is mutated
        # and returned rather than being constructed new.
        self.data = [
            Records._recordize(row_num_gen, record)
            for index, record in enumerate(self.data)
        ]

    @cached_property
    def headers(self):
        """
        >>> Records([{'id': 18, 'val': 'a'}]).headers
        ['id', 'val']
        """
        if self.data:
            return list(self.data[0].keys())
        else:
            raise PhaserError("Records initialized without data")

    @classmethod
    def _recordize(cls, number_generator, record):
        if isinstance(record, Record):
            return record
        if PHASER_ROW_NUM in record:
            record[PHASER_ROW_NUM] = int(record[PHASER_ROW_NUM])
            return Record(record[PHASER_ROW_NUM], record)
        return Record(next(number_generator), record)

    # Transform back into native list(dict)
    def to_records(self):
        """
        >>> Records([{'id': 18, 'val': 'a'}]).to_records()
        [{'id': 18, 'val': 'a'}]
        """
        return [ r.data for r in self.data ]


class Record(UserDict):
    def __init__(self, row_num, record):
        super().__init__(record)
        self.row_num = row_num

    def __repr__(self):
        return f"(row_num={self.row_num}, data={super().__repr__()})"
