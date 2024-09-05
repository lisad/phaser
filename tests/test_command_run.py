import argparse
import filecmp
from pathlib import Path
import pytest
import phaser.cli
from phaser import read_csv, PHASER_ROW_NUM

current_path = Path(__file__).parent

def __build_command():
    parser = argparse.ArgumentParser()
    parser.add_argument("-", "--verbose", action="store_true")
    command = phaser.cli.commands.RunPipelineCommand()
    command.add_arguments(parser)
    return (parser, command)

def test_runs_a_pipeline(tmpdir):
    (parser, command) = __build_command()
    source = current_path / "fixture_files" / "runner-test.csv"
    (args, extra) = parser.parse_known_args(f"passthrough {tmpdir} {source}".split())
    command.add_incremental_arguments(args, parser)
    command.execute(args)
    output = read_csv(tmpdir / "passthrough_output.csv")
    for row in output:
        del row[PHASER_ROW_NUM]
    assert output == read_csv(source)

@pytest.mark.parametrize("pipeline,exception",
    [
        ("pipelinerunnerpipeline", ModuleNotFoundError),
        ("doesnotexist", ModuleNotFoundError),
        ("nopipelinehere", Exception),
        ("multiplepipelines", Exception),
    ]
)
def test_failure_scenarios(tmpdir, pipeline, exception):
    (parser, command) = __build_command()
    source = current_path / "fixture_files" / "crew.csv"
    (args, extra) = parser.parse_known_args(f"{pipeline} {tmpdir} {source}".split())
    with pytest.raises(exception):
        command.add_incremental_arguments(args, parser)
        command.execute(args)

def test_overrides_working_directory():
    pass

def test_overrides_source():
    pass

def test_overrides_pipeline_package():
    pass
