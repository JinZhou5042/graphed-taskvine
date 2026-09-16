# Contributing

## Scope

Changes should preserve a narrow adapter boundary:

- graphed owns `Plan`, `Task`, `Partition`, IR, and reduction semantics;
- this repository owns translation to VineGraph and worker result/error transport;
- TaskVine owns scheduling, recovery, worker management, and data transfer.

Avoid copying optimizer or scheduler responsibilities into the adapter.

## Setup

Use Python 3.11 or newer and a TaskVine build that provides
`ndcctools.taskvine.vine_graph`:

```bash
python -m pip install -e '.[test]'
python -c "from ndcctools.taskvine.vine_graph import VineGraph, Workflow"
```

## Checks

Before opening a pull request:

```bash
ruff check .
ruff format --check .
pytest tests/test_contract.py
pytest tests/test_executor.py
```

The full executor suite uses VineGraph local execution. When `vine_worker` is available, also run:

```bash
GTV_WORKER=1 pytest tests/test_executor.py
```

## Compatibility checklist

An interface change must state:

1. which graphed `Plan` invariant changes;
2. which VineGraph call or parameter changes;
3. whether task ordering or reduction grouping changes;
4. whether retry, exception, or manager ownership semantics change;
5. how an existing caller migrates.

Prefer additive constructor options and capability detection over version checks. Pin a dependency
commit only when the required capability cannot be detected directly.

## Pull requests

Keep generated worker directories, data files, environments, logs, and benchmark outputs out of
Git. Include compact benchmark summaries only when the command, input shape, resources, and metric
definition are documented.
