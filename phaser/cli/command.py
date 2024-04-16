class Command:
    def add_arguments(self, parser):
        pass

    def has_incremental_arguments(self, args):
        """ True if more arguments may be needed after the original set are
        parsed. False otherwise."""
        return False

    def add_incremental_arguments(self, args, parser):
        """ Add any arguments that will only be knowable after the first set of
        arguments has been parsed. Those arguments would be the ones defined
        globally as well as any that were added by this command's
        `add_arguments` method.

        This method will only be called if `has_incremental_arguments` returns True.

        :param args: the arguments parsed so far
        :param parser: the argument parser for this specific command
        """
        pass

    def execute(self, args):
        raise NotImplementedError("'execute' must be implemented by a concrete subclass")
