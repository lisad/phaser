from phaser import Phase
import pytest
import os
from pathlib import Path

current_path = Path(__file__).parent

def test_load_and_save(tmpdir):
    class Transformer(Phase):
        source = current_path / "fixtures" / "employees.csv"
        working_dir = tmpdir

    Transformer().run()
    assert os.path.exists(os.path.join(tmpdir, "Transformer-employees.csv"))
