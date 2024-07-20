"""
A utility for various tasks useful when building and running Phaser data
integration pipelines
"""

import argparse
from importlib import import_module
import logging
import os
import pkgutil
import inspect
import phaser
import shutil
import sys
import traceback

def find_commands():
    path = os.path.dirname(__file__)
    command_dir = os.path.join(path, "commands")
    # Find all of the modules in the commands directory
    command_names = [
        name
        for _, name, is_pkg in pkgutil.iter_modules([command_dir])
        if not is_pkg
    ]
    # Load the module for each command
    commands = {
        name: load_command(name)
        for name in command_names
    }
    return commands

def load_command(name):
    module_name = f"phaser.cli.commands.{name}"
    module = import_module(module_name)
    # Use the module's explicit help text or its docstring for command help.
    help_desc = getattr(module, "__help__", None) or module.__doc__ or ''
    # Take the first line of the help text only, or if the result was an empty
    # string, then just use None so the default is used for the command help.
    help_text = help_desc.strip().split('\n')[0] or None

    def is_command(m):
        return (isinstance(m, type) and
                issubclass(m, phaser.cli.Command) and
                inspect.getmodule(m) == module)

    commands = inspect.getmembers(module, is_command)
    if len(commands) != 1:
        raise Exception(f"Found {len(commands)} commands declared in {module}")
    # Create a new instance of the command
    command = commands[0][1]()

    return {
        "module": module,
        "help_text": help_text,
        "help_desc": help_desc,
        "instance": command
    }

def main(argv):
    commands = find_commands()

    parser = argparse.ArgumentParser(
        prog="phaser",
        description=__doc__,
    )
    parser.add_argument(
        "-v", "--verbose",
        help="output more information during execution",
        action="store_true",
    )
    parser.add_argument(
        "-l", "--log",
        help="set the log level",
        default=logging.INFO,
        choices=[
                    "CRITICAL",
                    "DEBUG",
                    "ERROR",
                    "INFO",
                    "WARNING",
                    logging.CRITICAL,
                    logging.DEBUG,
                    logging.ERROR,
                    logging.INFO,
                    logging.WARNING,
                ]
    )
    subparsers = parser.add_subparsers(
        title="commands", dest="command"
    )
    for name in commands.keys():
        command = commands[name]
        subparser = subparsers.add_parser(
            name,
            help=command["help_text"],
            description=command["help_desc"],
            # Use the raw formatter to keep whitespace from the help text, in
            # case the developer did some nice indenting.
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        command["parser"] = subparser
        command["instance"].add_arguments(subparser)

    (args, extras) = parser.parse_known_args(argv)
    if not args.command:
        parser.print_help()
        sys.exit(2)

    loglevel = args.log
    if isinstance(loglevel, int):
        numeric_level = loglevel
    else:
        numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level)

    command = commands.get(args.command)
    cmd = command["instance"]
    if cmd.has_incremental_arguments(args):
        cmd.add_incremental_arguments(args, command["parser"])
        args = parser.parse_args(argv)
    try:
        cmd.execute(args)
    except phaser.DataException as e:
        print("\nPipeline run failed while processing data.  Errors and row numbers causing errors have been reported.")
    except phaser.PhaserError as e:
        print("\nPipeline run stopped due to logic error.")
        traceback.print_exc()
    except Exception as e:
        print(f"ERROR running '{args.command}': {sys.exception()}")
        if args.verbose:
            traceback.print_exc()
        print("-" * shutil.get_terminal_size().columns + "\n")
        command["parser"].print_help()
        sys.exit(1)
