"""Task-runner library context loader (pickled BY VALUE, so it runs before any import works)."""


def context_loader(graph_pkl):
    import os
    import sys

    import cloudpickle

    # Library inputs (the shipped graphed_taskvine package, analysis modules) land in the library
    # sandbox; make them importable before unpickling a graph that references them by import.
    sandbox = os.getcwd()
    if sandbox not in sys.path:
        sys.path.insert(0, sandbox)
    graph = cloudpickle.loads(graph_pkl)

    # Every function call forks from this library process, so anything done here is inherited by
    # all calls: unpickle each plan's (process, combine, empty) once and import graphed/awkward now,
    # instead of once per call.
    try:
        from graphed_taskvine import worker

        worker.prime(graph)
    except Exception as exc:  # priming is an optimization; calls still unpickle on demand
        print(f"graphed_taskvine: priming skipped: {exc!r}", file=sys.stderr)
    return {"graph": graph}
