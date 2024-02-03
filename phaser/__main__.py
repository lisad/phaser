# A script that loads the Phaser environment and runs a pipeline
#
# This is the prototype from which we can build more subcommands to use for
# running various tasks as part of a data integration system.

import sys
import phaser.cli

if __name__ == '__main__':
    # Do not pass in the name of the invoked program; just the args, ma'am.
    phaser.cli.main(sys.argv[1:])
