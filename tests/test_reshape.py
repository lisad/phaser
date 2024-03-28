import pytest
from pathlib import Path
from collections import defaultdict
from phaser import ReshapePhase, read_csv, Pipeline, dataframe_step, batch_step

current_path = Path(__file__).parent


@batch_step
def merge_by_location(row_data, context):
    """ Data coming into this step has multiple rows per subject, each with a different
    field and value reading. """
    fields_by_location = defaultdict(dict)  # first organize into a dict with one entry per subject
    for row in row_data:
        fields_by_location[row['location']][row['measure']] = row['value']
    location_list = []  # Now reorganize to a list of records with unique locations
    for location, field_dict in fields_by_location.items():
        location_list.append({'location': location, **field_dict})
    return location_list


def test_reshape():
    phase = ReshapePhase("myreshape", steps=[merge_by_location])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'locations.csv'))
    phase.run()
    assert len(phase.row_data) == 2
    assert phase.row_data == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


@dataframe_step
def df_transform(df, context):
    return df.pivot(index='location', columns='measure', values='value').reset_index()


def test_dataframe_phase(tmpdir):

    phase = ReshapePhase("PhaseWithDFStep", steps=[df_transform])
    phase.load_data(read_csv(current_path / 'fixture_files' / 'locations.csv'))
    results = phase.run()
    assert len(results) == 2
    assert results == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


def test_dataframe_phase_in_pipeline(tmpdir):
    phase = ReshapePhase("PhaseWithDFStep", steps=[df_transform])

    class MyPandasPipeline(Pipeline):
        source = current_path / 'fixture_files' / 'locations.csv'
        phases = [phase]

    pipeline = MyPandasPipeline(working_dir=tmpdir)
    pipeline.run()
    with open(tmpdir / 'PhaseWithDFStep_output_locations.csv') as f:
        line = f.readline()
        assert line == "location,gamma radiation,temperature\n"
        line = f.readline()
        assert line == "hangar deck,9.8 μR/h,16\n"
        line = f.readline()
        assert line == "main engineering,10.9 μR/h,22\n"


def test_reshape_explode(tmpdir):
    """ This test illustrates pandas explode, which is fun.  Also note it would be useful to have a multi-value
    column type that would automatically do the parsing done below where the string is split into a list -- not only
    to have the column convert type on loading, but also so we can save correctly (see
    [issue](https://github.com/lisad/phaser/issues/46) )  The file created in here should be converted to a fixture
    when that would be useful for testing MultiValueColumn """
    @dataframe_step
    def explode_step(df):
        df['languages'] = df['languages'].str.split(',')
        df = df.explode('languages')
        return df.rename(columns={'languages': 'language'})

    phase = ReshapePhase("explode", steps=[explode_step])
    with (open(tmpdir / 'languages.csv', 'w') as csv):
        csv.write("crew id,languages\n")
        csv.write('1,"Standard"\n')
        csv.write('2,"Standard,Vulcan,Romulan"\n')
        csv.write('3,"Standard,Klingon"\n')

    phase.load_data(read_csv(tmpdir / 'languages.csv'))
    results = phase.run()
    assert len(results) == 6
    assert results[5]['language'] == "Klingon"
