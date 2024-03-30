from pathlib import Path
import pytest
from phaser import Pipeline, Phase, batch_step


current_path = Path(__file__).parent


@pytest.mark.skip("I need to figure out how row number generation can start with the highest number loaded in")
def test_number_go_up(tmpdir):
    # Also we should have a test that loads in __phaser_row_num__ values already set
    @batch_step
    def adds_rows(batch, context):
        batch.append({'id': 100, 'name': "Fleet Victualling"})
        return batch

    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[adds_rows])])
    pipeline.run()
    new_row = list(filter(lambda row: row['id'] == 100, pipeline.phases[0].row_data))[0]
    print(new_row)
    assert new_row.row_num > 5