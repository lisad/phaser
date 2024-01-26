class Command:
    def add_arguments(self, parser):
        pass

    def execute(self, args):
        raise NotImplementedError("'execute' must be implemented by a concrete subclass")
