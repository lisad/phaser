import pytest
from phaser import Pipeline, Phase, flatten_column, JSON_RECORD_FORMAT, CSV_FORMAT
from test_csv import write_text
from phaser.io import save_json, read_csv


def test_extension_detected(tmpdir):
    write_text(tmpdir / 'testfile.json', '[{"id":"NCC1701-D","name":"Enterprise","class":"Galaxy"}]')
    pipeline = Pipeline(working_dir=tmpdir, source=tmpdir / 'testfile.json')
    data = pipeline.load(tmpdir / 'testfile.json')
    assert data[0]['id'] == "NCC1701-D"


SHIP_DATA = [
        {'id': "NCC1701-D", 'name': 'Enterprise', 'class': 'Galaxy', 'status': {'duty': 'museum', 'date': 2402}},
        {'id': "NCC-71099", 'name': 'Challenger', 'class': 'Galaxy', 'status': {'duty': 'active', 'date': 2378}}
    ]


def test_roundtrip(tmpdir):
    class MyPipeline(Pipeline):
        save_format = JSON_RECORD_FORMAT

    pipeline = MyPipeline(working_dir=tmpdir, source=tmpdir / 'testfile.json')
    pipeline.save(SHIP_DATA, tmpdir / 'testfile.json')
    loaded_data = pipeline.load(tmpdir / 'testfile.json')
    assert loaded_data == SHIP_DATA


def test_roundtrip_json_value_to_csv(tmpdir):
    # This test includes nested data fields which are saved in CSV (source copy) then re-opened and flattened
    save_json(tmpdir / 'ship_data.json', SHIP_DATA)

    class MyPipeline(Pipeline):
        save_format = CSV_FORMAT

    phase = Phase(steps=[flatten_column('status')])
    pipeline = MyPipeline(working_dir=tmpdir, source=tmpdir / 'ship_data.json', phases=phase)
    pipeline.run()
    assert pipeline.source_copy_filename().endswith('csv')
    final_data = read_csv(tmpdir / pipeline.phase_save_filename(phase))
    assert all(int(row['status__date']) > 1000 for row in final_data)
