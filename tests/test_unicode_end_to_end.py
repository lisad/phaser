import os
import pytest
from pathlib import Path
from phaser import (
    Phase,
    Pipeline,
    row_step,
)
from phaser.io import (ExtraMapping, read_csv)

current_path = Path(__file__).parent

@row_step(extra_sources = ['currency_names'])
def add_currency_names(row, currency_names):
    fs = row['from']
    fn = currency_names[fs]
    ts = row['to']
    tn = currency_names[ts]
    row['from_name'] = fn
    row['to_name'] = tn
    return row

class EnrichPhase(Phase):
    steps = [ add_currency_names ]
    extra_sources = [ ExtraMapping('currency_names') ]

class MyPipeline(Pipeline):
    phases = [ EnrichPhase('add_currency_names') ]

def test_pipeline(tmpdir):
    source = current_path / 'fixture_files' / 'exchange_rates.csv'
    pipeline = MyPipeline(source=source, working_dir=tmpdir)
    pipeline.init_source('currency_names',
                         current_path / 'fixture_files' / 'currency_names.csv')
    pipeline.run()


    output = tmpdir / 'add_currency_names_output.csv'
    assert os.path.exists(output)
    data = read_csv(output)
    # extract the from and to symbols into tuples so we can compare to what we
    # expect relatively easily
    symbols = map(lambda x: (x['from'], x['to']), data)
    assert [x for x in symbols] == [
        ('$','€'),
        ('¥','¢'),
        ('௹','฿'),
        ('£','₱'),
    ]
