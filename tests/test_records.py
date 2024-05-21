import pytest
from phaser.records import Records
from phaser.constants import PHASER_ROW_NUM
from phaser.exceptions import PhaserError


def test_records_fail_bogus_row_nums():
    with pytest.raises(PhaserError):
        Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: "BOGUS"}])


def test_records_fail_bool_row_nums():
    with pytest.raises(PhaserError):
        Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: True}])


def test_records_fail_zero_row_nums():
    with pytest.raises(PhaserError):
        Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: "0"}])
    with pytest.raises(PhaserError):
        Records([{'id': 18, 'val': 'a', PHASER_ROW_NUM: 0}])

