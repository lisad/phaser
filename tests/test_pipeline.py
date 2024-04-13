from pathlib import Path
import pytest
from phaser import Pipeline, Phase, batch_step


current_path = Path(__file__).parent


# LMDTODO: Move more pipeline tests out of test_basics and call that test_phase
def test_number_go_up(tmpdir):
    @batch_step
    def adds_row(batch, context):
        batch.append({'key': "Fleet Victualling", 'value': 100})
        return batch

    # Also we should have a test that loads in __phaser_row_num__ values already set
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[adds_row])])
    pipeline.run()
    new_row = list(filter(lambda row: row['value'] == 100, pipeline.phases[0].row_data))[0]
    assert new_row.row_num > 5


