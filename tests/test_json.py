import pytest
from phaser import Pipeline, JSON_RECORD_FORMAT
from test_csv import write_text


def test_extension_detected(tmpdir):
    write_text(tmpdir / 'testfile.json', '[{"id":"NCC1701-D","name":"Enterprise","class":"Galaxy"}]')
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'testfile.json')
    data = pipeline.load(tmpdir / 'testfile.json')
    assert data[0]['id'] == "NCC1701-D"


def test_roundtrip(tmpdir):
    class MyPipeline(Pipeline):
        save_format = JSON_RECORD_FORMAT

    pipeline = MyPipeline(working_dir=tmpdir, source=tmpdir / 'testfile.json')
    data = [
        {'id': "NCC1701-D", 'name': 'Enterprise', 'class': 'Galaxy'},
        {'id': "NCC-71099", 'name': 'Challenger', 'class': 'Galaxy'}
    ]
    pipeline.save(data, tmpdir / 'testfile.json')
    loaded_data = pipeline.load(tmpdir / 'testfile.json')
    assert data == loaded_data
