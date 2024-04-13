import os
import pytest
from phaser.io import ExtraRecords

def test_basic_save(tmpdir):
    data = [{ 'num': 12, 'name': 'A dozen'}, { 'num': 43, 'name': 'One more than the answer to life'}]
    mapping = ExtraRecords('numbers', data)
    dest = tmpdir / 'numbers.csv'
    mapping.save(dest)
    assert os.path.exists(dest)
    text = dest.read_text(encoding='utf8')
    assert text == 'num,name\n12,A dozen\n43,One more than the answer to life\n'

def test_basic_load(tmpdir):
    data = [{ 'num': '12', 'name': 'A dozen'}, { 'num': '43', 'name': 'One more than the answer to life'}]
    text = 'num,name\n12,A dozen\n43,One more than the answer to life\n'
    source = tmpdir / 'numbers.csv'
    source.write_text(text, encoding='utf8')
    mapping = ExtraRecords('numbers')
    mapping.load(source)
    assert mapping.data == data
