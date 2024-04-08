from pathlib import Path
import pytest
from phaser import Pipeline, Phase, batch_step
from steps import adds_row


current_path = Path(__file__).parent


# LMDTODO: Move more pipeline tests out of test_basics and call that test_phase
def test_number_go_up(tmpdir):
    pipeline = Pipeline(working_dir=tmpdir,
                        source=(current_path / 'fixture_files' / 'departments.csv'),
                        phases=[Phase(steps=[adds_row])])
    pipeline.run()
    new_row = list(filter(lambda row: row['id'] == 100, pipeline.phases[0].row_data))[0]
    assert new_row.row_num > 5


