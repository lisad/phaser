from phaser import Phase
import pytest
import os

def test_load_and_save():
    class Transformer(Phase):
        source = "fixtures/employees.csv"
        working_dir = "tmp"

    Transformer().run()
    assert os.path.exists("tmp/Transformer-employees.csv")