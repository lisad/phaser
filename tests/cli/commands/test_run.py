import argparse
import filecmp
from pathlib import Path
import os
import pytest
import phaser.cli

test_dir = Path(__file__).parent
current_path = Path(__file__).parent.parent.parent

def __build_command():
    parser = argparse.ArgumentParser()
    command = phaser.cli.commands.RunPipelineCommand()
    command.add_arguments(parser)
    return (parser, command)

def test_runs_a_pipeline(tmpdir):
    cwd = Path(os.getcwd())
    try:
        os.chdir(test_dir)
        (parser, command) = __build_command()
        source = current_path / "fixture_files" / "runner-test.csv"
        args = parser.parse_args(f"passthrough {tmpdir} {source}".split())
        command.execute(args)
        # Check that the output in the tmpdir is exactly the same as the input
        assert filecmp.cmp(Path(tmpdir) / "passthrough_output_runner-test.csv", source)
    finally:
        os.chdir(cwd)

def test_failure_scenarios(tmpdir):
    # Test tables are helpful for reducing code, but I am not sure how to get
    # the name of the test case that failed when a test case fails. In other
    # ecosystems, the assertions accept a message upon failure that I would pass
    # the case name into.
    tests = [{
        "name": "No pipeline package found",
        "pipeline": "pipelinerunnerpipeline",
        "exception": ModuleNotFoundError
    }, {
        "name": "No pipeline module found",
        "cwd": test_dir,
        "pipeline": "doesnotexist",
        "exception": ModuleNotFoundError
    }, {
        "name": "No pipeline found",
        "cwd": test_dir,
        "pipeline": "nopipelinehere",
        "exception": Exception
    }, {
        "name": "Multiple pipelines found",
        "cwd": test_dir,
        "pipeline": "multiplepipelines",
        "exception": Exception
    }]
    for test in tests:
        cwd = Path(os.getcwd())
        try:
            if test.get('cwd'):
                os.chdir(test['cwd'])
            (parser, command) = __build_command()
            source = current_path / "fixture_files" / "crew.csv"
            args = parser.parse_args(f"{test['pipeline']} {tmpdir} {source}".split())
            with pytest.raises(test['exception']):
                command.execute(args)
        finally:
            # Be sure to return to the directory we were at at the beginning
            os.chdir(cwd)

def test_overrides_working_directory():
    pass

def test_overrides_source():
    pass

def test_overrides_pipeline_package():
    pass
