import argparse
from pathlib import Path
import os
import pytest
import phaser.cli

current_path = Path(__file__).parent.parent.parent
print(f"{current_path}")

def __build_command():
    parser = argparse.ArgumentParser()
    command = phaser.cli.commands.RunPipelineCommand()
    command.add_arguments(parser)
    return (parser, command)

def test_runs_a_pipeline():
    pass

def test_no_pipeline_package_found(tmpdir):
    """
    No pipelines packaage is found, because the test is run from the main
    directory, which has no "pipelines" subdirectory
    """
    (parser, command) = __build_command()
    source = current_path / "fixture_files" / "crew.csv"
    args = parser.parse_args(f"pipelinerunnerpipeline {tmpdir} {source}".split())
    with pytest.raises(ModuleNotFoundError):
        command.execute(args)

def test_no_pipeline_module_found(tmpdir):
    """
    Test is run from the testing directory that has a "pipelines" package, but
    there is no matching module.
    """
    cwd = Path(os.getcwd())
    try:
        os.chdir(Path(__file__).parent)
        (parser, command) = __build_command()
        source = current_path / "fixture_files" / "crew.csv"
        args = parser.parse_args(f"doesnotexist {tmpdir} {source}".split())
        with pytest.raises(ModuleNotFoundError):
            command.execute(args)
    finally:
        # Be sure to change back to the prior working directory
        os.chdir(cwd)

def test_no_pipeline_found(tmpdir):
    """
    Test is run from the testing directory that has a "pipelines" package, but
    there is pipeline class declared in the module
    """
    cwd = Path(os.getcwd())
    try:
        os.chdir(Path(__file__).parent)
        (parser, command) = __build_command()
        source = current_path / "fixture_files" / "crew.csv"
        args = parser.parse_args(f"nopipelinehere {tmpdir} {source}".split())
        with pytest.raises(Exception):
            command.execute(args)
    finally:
        # Be sure to change back to the prior working directory
        os.chdir(cwd)

def test_multiple_pipelines_found(tmpdir):
    """
    Test is run from the testing directory that has a "pipelines" package, but
    there are multiple pipeline classes declared in the module
    """
    cwd = Path(os.getcwd())
    try:
        os.chdir(Path(__file__).parent)
        (parser, command) = __build_command()
        source = current_path / "fixture_files" / "crew.csv"
        args = parser.parse_args(f"multiplepipelines {tmpdir} {source}".split())
        with pytest.raises(Exception):
            command.execute(args)
    finally:
        # Be sure to change back to the prior working directory
        os.chdir(cwd)

def test_overrides_working_directory():
    pass

def test_overrides_source():
    pass

def test_overrides_pipeline_package():
    pass
