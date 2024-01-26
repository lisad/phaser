import argparse
from importlib import import_module
import os
import pkgutil
import inspect
import phaser

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
    help_text = getattr(module, "__help__", None) or module.__doc__ or ''
    # Take the first line of the help text only, or if the result was an empty
    # string, then just use None so the default is used for the command help.
    help_text = help_text.strip().split('\n')[0] or None

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
        "instance": command
    }

def main(argv):
    commands = find_commands()

    parser = argparse.ArgumentParser(prog="phaser")
    subparsers = parser.add_subparsers(
        title="commands", dest="command"
    )
    for name in commands.keys():
        command = commands[name]
        subparser = subparsers.add_parser(name, help=command["help_text"])
        command["instance"].add_arguments(subparser)

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        exit()

    print(f"Running {args.command}")
    commands[args.command]["instance"].execute(args)
