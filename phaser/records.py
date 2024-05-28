from collections import UserDict, UserList
from functools import cached_property
from .exceptions import PhaserError


# Defined twice to avoid circular import - if we keep this overall approach without further refactoring
# that makes this moot, we can fix this later
PHASER_ROW_NUM = '__phaser_row_num__'


def row_num_generator(start_from=1):
    value = start_from
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
        >>> str(Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: '2'}]))
        "[(row_num=2, data={'id': 18, 'val': 'a'})]"
        """
        number_from = kwargs.get('number_from', 1)
        self.row_num_gen = row_num_generator(start_from=number_from)
        self.max_row_num = number_from

        # NOrmally records will try to prserve row numbers, but in reshape/explode phases, we may
        # explicitly renumber all rows (preserve_numbers=False)
        self.preserve_numbers = kwargs.get('preserve_numbers', True)

        super().__init__(args[0] if args else None)
        # Slicing a UserList results in constructing a brand new list, which
        # would reset the row_num for our records if we were to recreated them
        # from scratch. But if the elements of the incoming list are already
        # `PhaseRecord`s, then just leave them alone.
        # This is also generally helpful in steps where the record is mutated
        # and returned rather than being constructed new.
        self.data = [
            self._recordize(record)
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

    def get_max_row_num(self):
        return self.max_row_num

    def _recordize(self, record):
        def fix_row_num(row_num):
            if row_num is None:
                return next(self.row_num_gen)
            if not isinstance(row_num, bool) and isinstance(row_num, int) and row_num > 0:
                return row_num
            elif isinstance(row_num, str):
                try:
                    if int(row_num) > 0:
                        return int(row_num)
                except ValueError:
                    pass # to phaser error

            raise PhaserError(f"Data received with {PHASER_ROW_NUM} value not an integer")

        if isinstance(record, Record):
            if not self.preserve_numbers:
                record.row_num = next(self.row_num_gen)
                self.max_row_num = record.row_num
            return record

        else:
            if PHASER_ROW_NUM in record and self.preserve_numbers:
                row_num = fix_row_num(record.pop(PHASER_ROW_NUM))
            else:
                row_num = next(self.row_num_gen)

            if row_num > self.max_row_num:
                self.max_row_num = row_num
            return Record(row_num, record)

    # Transform back into native list(dict)
    def to_records(self):
        """
        >>> Records([{'id': 18, 'val': 'a'}]).to_records()
        [{'id': 18, 'val': 'a'}]
        """
        return [ r.data for r in self.data ]

    def for_save(self):
        """
        >>> Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: 1}]).for_save()
        [{'id': 18, 'val': 'a', '__phaser_row_num__': 1}]
        """
        return [ {**r.data, PHASER_ROW_NUM: r.row_num} for r in self.data ]


class Record(UserDict):
    def __init__(self, row_num, record):
        super().__init__(record)
        self.row_num = int(row_num)

    def __repr__(self):
        return f"(row_num={self.row_num}, data={super().__repr__()})"
