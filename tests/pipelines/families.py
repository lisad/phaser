from collections import defaultdict
from phaser import (
    Column,
    IntColumn,
    ON_ERROR_STOP_NOW,
    Phase,
    Pipeline,
    row_step)
from phaser.io import ExtraMapping


@row_step(extra_outputs=['sibling_counts'])
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
    extra_outputs = [ExtraMapping('sibling_counts', defaultdict(int))]
    steps = [increment_counts]


@row_step(extra_sources=['sibling_counts'])
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
    extra_sources = ['sibling_counts']
    steps = [merge_counts]


class PipePipeline(Pipeline):
    phases = [ CountParents, EnrichSiblings ]
