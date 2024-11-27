import os
import pytest
from phaser.io import ExtraMapping

TABULAR_DATA = [{'key': 12, 'value': 'A dozen'},
                {'key': 43, 'value': 'One more than the answer to life'}]

DICT_DATA = {12: 'A dozen', 43: 'One more than the answer to life'}

def test_transform_to_tabular():
    mapping = ExtraMapping('numbers', DICT_DATA)
    assert mapping.prepare_for_save() == TABULAR_DATA


def test_load_from_tabular(tmpdir):
    mapping = ExtraMapping('numbers')
    mapping.load_data(TABULAR_DATA)
    assert mapping.data == DICT_DATA
