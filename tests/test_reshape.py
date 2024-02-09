import pytest
from pathlib import Path
from collections import defaultdict
from pandas import DataFrame, pivot, read_csv
from phaser import ReshapePhase, DataFramePhase

current_path = Path(__file__).parent

def test_reshape(tmpdir):

    class MyReshape(ReshapePhase):
        def reshape(self):
            """ Data coming into this reshape has multiple rows per subject, each with a different
            field and value reading. """
            fields_by_location = defaultdict(dict)   #first organize into a dict with one entry per subject
            for row in self.row_data:
                fields_by_location[row['location']][row['measure']] = row['value']
            location_list = []   # Now reorganize to a list of records with unique locations
            for location, field_dict in fields_by_location.items():
                location_list.append({'location': location, **field_dict})
            return location_list

    phase = MyReshape("myreshape")
    phase.run(current_path / 'fixture_files' / 'locations.csv', tmpdir/'output.csv')
    assert len(phase.row_data) == 2
    assert phase.row_data == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


def test_reshape_pandas(tmpdir):
    class MyPandasPhase(DataFramePhase):
        def df_transform(self, df):
            return df.pivot(index='location', columns='measure', values='value').reset_index()

        dataframe_fn = df_transform

    phase = MyPandasPhase("MyPandasPhase")
    phase.run(current_path / 'fixture_files' / 'locations.csv', tmpdir / 'output.csv')
    assert len(phase.row_data) == 2
    print(phase.row_data)
    assert phase.row_data == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]
