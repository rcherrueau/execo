class ProcessesFailed(Exception):
    """Raised when one or more `execo.process.Process` have failed."""

    def __init__(self, processes):
        """:param processes: iterable of failed processes"""
        self._processes = processes

    def __str__(self):
        s = "<ProcessesFailed> - failed process(es):\n"
        for p in self._processes:
            s += " " + p.dump() + "\n"
            return s
