import pytest
from pathlib import Path
from collections import defaultdict
from phaser import Phase, read_csv, Pipeline, dataframe_step, batch_step, PHASER_ROW_NUM

fixture_path = Path(__file__).parent / 'fixture_files'


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


def test_reshape_renumber():
    phase = Phase("myreshape", steps=[merge_by_location], renumber=True)
    phase.load_data(read_csv(fixture_path / 'locations.csv'))
    phase.run()
    assert len(phase.row_data) == 2
    assert phase.row_data == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


@dataframe_step
def df_transform(df, context):
    return df.pivot(index='location', columns='measure', values='value').reset_index()


def test_dataframe_step_and_renumber(tmpdir):
    phase = Phase(name="PhaseWithDFStep", steps=[df_transform], renumber=True)
    phase.load_data(read_csv(fixture_path / 'locations.csv'))
    results = phase.run()
    assert len(results) == 2
    assert results == [
        {'location': 'hangar deck', 'temperature': '16', 'gamma radiation': '9.8 μR/h'},
        {'location': 'main engineering', 'temperature': '22', 'gamma radiation': '10.9 μR/h'}
    ]


def test_reshape_renumber_pipeline_output(tmpdir):
    phase = Phase("PhaseWithDFStep", steps=[df_transform], renumber=True)

    class MyPandasPipeline(Pipeline):
        source = fixture_path / 'locations.csv'
        phases = [phase]

    pipeline = MyPandasPipeline(working_dir=tmpdir)
    pipeline.run()
    with open(tmpdir / 'PhaseWithDFStep_output.csv') as f:
        line = f.readline()
        assert line == f"location,gamma radiation,temperature,{PHASER_ROW_NUM}\n"
        line = f.readline()
        assert line == "hangar deck,9.8 μR/h,16,1\n"
        line = f.readline()
        assert line == "main engineering,10.9 μR/h,22,2\n"


@dataframe_step
def explode_step(df):
    df['languages'] = df['languages'].str.split(',')
    df = df.explode('languages')
    return df.rename(columns={'languages': 'language'})


def test_explode(tmpdir):
    """ This test illustrates pandas explode, which is fun.  Also note it would be useful to have a multi-value
    column type that would automatically do the parsing done below where the string is split into a list -- not only
    to have the column convert type on loading, but also so we can save correctly (see
    [issue](https://github.com/lisad/phaser/issues/46) )  The file created in here should be converted to a fixture
    when that would be useful for testing MultiValueColumn """
    phase = Phase("explode", steps=[explode_step], renumber=True)
    phase.load_data(read_csv(fixture_path / 'languages.csv'))
    results = phase.run()
    assert len(results) == 6
    assert results[5]['language'] == "Klingon"
    row_nums = [record.row_num for record in results]
    assert len(set(row_nums)) == len(row_nums)


def test_add_rows_add_numbers():
    @batch_step
    def add_row(batch, context):
        batch.append({'deck': 5, 'location': 'secret lounge'})
        return batch

    # Test that numbering of new rows goes up
    phase = Phase(name='add_stuff', steps=[add_row, add_row])
    phase.load_data([{'deck': 10, 'location': '10 Forward'}])
    results = phase.run()
    row_nums = [record.row_num for record in results]
    assert len(set(row_nums)) == len(row_nums)
