import argparse
import filecmp
from pathlib import Path
import pytest
import phaser.cli

current_path = Path(__file__).parent

def __build_command():
    parser = argparse.ArgumentParser()
    command = phaser.cli.commands.RunPipelineCommand()
    command.add_arguments(parser)
    return (parser, command)

def test_runs_a_pipeline(tmpdir):
    (parser, command) = __build_command()
    source = current_path / "fixture_files" / "runner-test.csv"
    args = parser.parse_args(f"passthrough {tmpdir} {source}".split())
    command.execute(args)
    # Check that the output in the tmpdir is exactly the same as the input
    assert filecmp.cmp(Path(tmpdir) / "passthrough_output_runner-test.csv", source)

@pytest.mark.parametrize("pipeline,exception",
    [
        ("pipelinerunnerpipeline", ModuleNotFoundError),
        ("doesnotexist", ModuleNotFoundError),
        ("nopipelinehere", Exception),
        ("multiplepipelines", Exception),
    ]
)
def test_failure_scenarios_p(tmpdir, pipeline, exception):
    (parser, command) = __build_command()
    source = current_path / "fixture_files" / "crew.csv"
    args = parser.parse_args(f"{pipeline} {tmpdir} {source}".split())
    with pytest.raises(exception):
        command.execute(args)

def test_overrides_working_directory():
    pass

def test_overrides_source():
    pass

def test_overrides_pipeline_package():
    pass
