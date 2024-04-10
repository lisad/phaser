import os
import pytest
from phaser.io import ExtraMapping

def test_basic_save(tmpdir):
    data = { 12: 'A dozen', 43: 'One more than the answer to life' }
    mapping = ExtraMapping('numbers', data)
    dest = tmpdir / 'numbers.csv'
    mapping.save(dest)
    assert os.path.exists(dest)
    text = dest.read_text(encoding='utf8')
    assert text == 'key,value\n12,A dozen\n43,One more than the answer to life\n'

def test_basic_load(tmpdir):
    data = { '12': 'A dozen', '43': 'One more than the answer to life' }
    text = 'key,value\n12,A dozen\n43,One more than the answer to life\n'
    source = tmpdir / 'numbers.csv'
    source.write_text(text, encoding='utf8')
    mapping = ExtraMapping('numbers')
    mapping.load(source)
    assert mapping.data == data

def test_object_save(tmpdir):
    """ Tests what happens when the value in a key/value mapping is an object
    rather than a scalar value."""
    data = { 12: {'num': 12, 'name': 'A dozen'}, 43: {'num': 43, 'name': 'One more than the answer to life'} }
    mapping = ExtraMapping('numbers', data)
    dest = tmpdir / 'numbers.csv'
    mapping.save(dest)
    assert os.path.exists(dest)
    text = dest.read_text(encoding='utf8')
    assert text == """key,value\n12,"{'num': 12, 'name': 'A dozen'}"\n43,"{'num': 43, 'name': 'One more than the answer to life'}"\n"""

def test_object_load(tmpdir):
    data = { '12': "{'num': 12, 'name': 'A dozen'}", '43': "{'num': 43, 'name': 'One more than the answer to life'}" }
    text = """key,value\n12,"{'num': 12, 'name': 'A dozen'}"\n43,"{'num': 43, 'name': 'One more than the answer to life'}"\n"""
    source = tmpdir / 'numbers.csv'
    source.write_text(text, encoding='utf8')
    mapping = ExtraMapping('numbers')
    mapping.load(source)
    assert mapping.data == data

@pytest.mark.skip("More complex object deserialization is not yet implemented")
def test_object_load_deserialization(tmpdir):
    data = { 12: {'num': 12, 'name': 'A dozen'}, 43: {'num': 43, 'name': 'One more than the answer to life'} }
    text = """key,value\n12,"{'num': 12, 'name': 'A dozen'}"\n43,"{'num': 43, 'name': 'One more than the answer to life'}"\n"""
    source = tmpdir / 'numbers.csv'
    source.write_text(text, encoding='utf8')
    mapping = ExtraMapping('numbers')
    mapping.load(source)
    assert mapping.data == data
