from pathlib import Path
import factory
from pytest import TempPathFactory
from phaser import Phase

class TransformEmployeesFactory(factory.Factory):
    class Transformer(Phase):
        source = Path(__file__).parent / "fixtures" / "employees.csv"
        working_dir = TempPathFactory.mktemp("working_dir")

