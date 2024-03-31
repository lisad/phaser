# Tests the cases where outputs from prior phases are used as sources to later
# phases.
from collections import defaultdict
import os
import pytest
from pathlib import Path
from phaser import (
    Column,
    context_step,
    IntColumn,
    Phase,
    Pipeline,
    row_step,
)
from phaser.io import read_csv, save_csv

@row_step
def increment_counts(row, context):
    parent_id = row['parent_id']
    counts = context.get('counts')
    counts[parent_id] += 1
    return row

# TODO: this is an annoying and repetitive reshaping of data that needs to be
# done for creating outputs and for reading in sources. Add functionality to
# Phaser to make this be easier to manage. Perhaps an annotation for the step or
# the phase?
def counts_to_output(counts):
    return [
        { 'parent_id': key, 'sibling_count': value }
        for key, value in counts.items()
    ]

# TODO: Same comment as for `counts_to_outputs` -- build features into Phaser to
# make this not have to be written all the time.
def source_to_counts(rows):
    return {
        row['parent_id']: row['sibling_count'] for row in rows
    }

@context_step
def output_counts(context):
    counts = context.get('counts')
    context.set_output('sibling_counts', counts_to_output(counts))

class CountParents(Phase):
    columns = [
        IntColumn('id'),
        Column('name'),
        IntColumn('parent_id'),
    ]
    extra_outputs = [ 'sibling_counts' ]
    steps = [ increment_counts, output_counts ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context.add_variable('counts', defaultdict(int))

@context_step
def prep_counts(context):
    source = context.get_source('sibling_counts')
    context.add_variable('counts', source_to_counts(source))

@row_step
def merge_counts(row, context):
    counts = context.get('counts')
    parent_id = row['parent_id']
    count = counts[parent_id]
    row['siblings'] = count-1
    return row

class EnrichSiblings(Phase):
    columns = [
        IntColumn('id'),
        Column('name'),
        IntColumn('parent_id'),
        IntColumn('siblings', required=False),
    ]
    extra_sources = [ 'sibling_counts' ]
    steps = [ prep_counts, merge_counts ]

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

    assert os.path.exists(tmpdir / 'EnrichSiblings_output_source.csv')

    output = read_csv(tmpdir / 'EnrichSiblings_output_source.csv')
    assert len(output) == 6
    expected_siblings = [ 2, 1, 0, 2, 2, 1 ]
    for i, d in enumerate(output):
        assert int(d['siblings']) == expected_siblings[i]
