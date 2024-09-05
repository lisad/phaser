# Tests the cases where outputs from prior phases are used as sources to later
# phases.
from collections import defaultdict
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
)
from phaser.io import read_csv, save_csv, ExtraMapping

@row_step(extra_outputs=[ 'sibling_counts' ])
def increment_counts(row, sibling_counts):
    parent_id = row['parent_id']
    sibling_counts[parent_id] += 1
    return row

class CountParents(Phase):
    error_policy = ON_ERROR_STOP_NOW
    columns = [
        IntColumn('id'),
        Column('name'),
        IntColumn('parent_id'),
    ]
    extra_outputs = [ ExtraMapping('sibling_counts', defaultdict(int)) ]
    steps = [ increment_counts ]

@row_step(extra_sources=[ 'sibling_counts' ])
def merge_counts(row, sibling_counts):
    parent_id = row['parent_id']
    count = sibling_counts[parent_id]
    row['siblings'] = count-1
    return row

class EnrichSiblings(Phase):
    error_policy = ON_ERROR_STOP_NOW
    columns = [
        IntColumn('id'),
        Column('name'),
        IntColumn('parent_id'),
        IntColumn('siblings', required=False),
    ]
    extra_sources = [ 'sibling_counts' ]
    steps = [ merge_counts ]

class PipePipeline(Pipeline):
    phases = [ CountParents, EnrichSiblings ]

def test_pipeline(tmpdir):
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
    pipeline = PipePipeline(source=source, working_dir=tmpdir)
    pipeline.run()

    assert os.path.exists(tmpdir / 'EnrichSiblings_output.csv')

    output = read_csv(tmpdir / 'EnrichSiblings_output.csv')
    assert len(output) == 6
    expected_siblings = [ 2, 1, 0, 2, 2, 1 ]
    for i, d in enumerate(output):
        assert int(d['siblings']) == expected_siblings[i]
