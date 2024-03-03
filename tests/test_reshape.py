import pytest
from pathlib import Path
from collections import defaultdict
import pandas as pd
from phaser import ReshapePhase, DataFramePhase, read_csv

current_path = Path(__file__).parent

def test_reshape():

    class MyReshape(ReshapePhase):
        def reshape(self, row_data):
            """ Data coming into this reshape has multiple rows per subject, each with a different
            field and value reading. """
            fields_by_location = defaultdict(dict)   #first organize into a dict with one entry per subject
            for row in row_data:
                fields_by_location[row['location']][row['measure']] = row['value']
            location_list = []   # Now reorganize to a list of records with unique locations
            for location, field_dict in fields_by_location.items():
                location_list.append({'location': location, **field_dict})
            return location_list

    phase = MyReshape("myreshape")
    phase.load_data(read_csv(current_path / 'fixture_files' / 'locations.csv'))
    phase.run()
    assert len(phase.row_data) == 2
    assert phase.row_data == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


def test_dataframe_phase(tmpdir):
    class MyPandasPhase(DataFramePhase):
        def df_transform(self, df):
            return df.pivot(index='location', columns='measure', values='value').reset_index()

    phase = MyPandasPhase("MyPandasPhase")
    phase.load_data(read_csv(current_path / 'fixture_files' / 'locations.csv'))
    results = phase.run()
    assert len(results) == 2
    assert results == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


def test_reshape_explode(tmpdir):
    """ This test illustrates pandas explode, which is fun.  Also note it would be useful to have a multi-value
    column type that would automatically do the parsing done below where the string is split into a list -- not only
    to have the column convert type on loading, but also so we can save correctly (see
    [issue](https://github.com/lisad/phaser/issues/46) )  The file created in here should be converted to a fixture
    when that would be useful for testing MultiValueColumn """
    class ExplodeListValues(DataFramePhase):
        def df_transform(self, df):
            df['languages'] = df['languages'].str.split(',')
            df = df.explode('languages')
            return df.rename(columns={'languages': 'language'})

    phase = ExplodeListValues("explode")
    with (open(tmpdir / 'languages.csv', 'w') as csv):
        csv.write("crew id,languages\n")
        csv.write('1,"Standard"\n')
        csv.write('2,"Standard,Vulcan,Romulan"\n')
        csv.write('3,"Standard,Klingon"\n')

    phase.load_data(read_csv(tmpdir / 'languages.csv'))
    results = phase.run()
    assert len(results) == 6
    assert results[5]['language'] == "Klingon"
