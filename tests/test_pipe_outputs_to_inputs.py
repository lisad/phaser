# Tests the cases where outputs from prior phases are used as sources to later
# phases.
import os
import pytest
from pathlib import Path
from phaser import (
    Column,
    IntColumn,
    ON_ERROR_STOP_NOW,
    Phase,
    Pipeline,
    row_step,
    CSV_FORMAT, JSON_RECORD_FORMAT
)
from phaser.io import save_csv, ExtraMapping
from pipelines.families import PipePipeline


@pytest.mark.parametrize("save_format, extension", [(JSON_RECORD_FORMAT, 'json'), (CSV_FORMAT, 'csv')])
def test_pipeline(tmpdir, save_format, extension):
    data = [
        {'id': 1, 'name': 'Alyx Barta', 'parent_id': 2},
        {'id': 2, 'name': 'Angele Hardie', 'parent_id': 1},
        {'id': 3, 'name': 'Flavius Pace', 'parent_id': 3},
        {'id': 4, 'name': 'Risko Roy', 'parent_id': 2},
        {'id': 5, 'name': 'Cristoforo Stephenson', 'parent_id': 2},
        {'id': 6, 'name': 'Zonibia Miyashita', 'parent_id': 1},
    ]
    source = Path(tmpdir / 'source.csv')
    save_csv(source, data)
    PipePipeline.save_format = save_format   # Monkey patching for quicker test
    pipeline = PipePipeline(source=source, working_dir=tmpdir)
    print(pipeline.context.rwos)
    pipeline.run()

    print(pipeline.context.rwos['sibling_counts'].data)
    print(os.listdir(tmpdir))
    assert os.path.exists(tmpdir / f"sibling_counts.{extension}")  # Intermediate results get saved in csv/json too
    print((tmpdir / f"sibling_counts.{extension}").read_text(encoding='utf8'))
    assert os.path.exists(tmpdir / f"EnrichSiblings_output.{extension}")
    output = pipeline.load(tmpdir / f"EnrichSiblings_output.{extension}")
    assert len(output) == 6
    expected_siblings = [ 2, 1, 0, 2, 2, 1 ]
    for i, d in enumerate(output):
        assert int(d['siblings']) == expected_siblings[i]
    assert False

@pytest.mark.parametrize("save_format, extension", [(JSON_RECORD_FORMAT, 'json'), (CSV_FORMAT, 'csv')])
def test_pipeline(tmpdir, save_format, extension):
    data = [
        {'id': 1, 'name': 'Alyx Barta', 'parent_id': 2},
        {'id': 2, 'name': 'Angele Hardie', 'parent_id': 1},
        {'id': 3, 'name': 'Flavius Pace', 'parent_id': 3},
        {'id': 4, 'name': 'Risko Roy', 'parent_id': 2},
        {'id': 5, 'name': 'Cristoforo Stephenson', 'parent_id': 2},
        {'id': 6, 'name': 'Zonibia Miyashita', 'parent_id': 1},
    ]
    source = Path(tmpdir / 'source.csv')
    save_csv(source, data)
    PipePipeline.save_format = save_format   # Monkey patching for quicker test
    pipeline = PipePipeline(source=source, working_dir=tmpdir)
    pipeline.run()

    assert os.path.exists(tmpdir / f"sibling_counts.{extension}")  # Intermediate results get saved in csv/json too
    assert os.path.exists(tmpdir / f"EnrichSiblings_output.{extension}")
    output = pipeline.load(tmpdir / f"EnrichSiblings_output.{extension}")
    assert len(output) == 6
    expected_siblings = [ 2, 1, 0, 2, 2, 1 ]
    for i, d in enumerate(output):
        assert int(d['siblings']) == expected_siblings[i]
