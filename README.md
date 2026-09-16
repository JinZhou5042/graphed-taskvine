# graphed-taskvine

`graphed-taskvine` runs a [`graphed`](https://github.com/graphed-org/graphed) `Plan` on
[TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) through the VineGraph interface.
It keeps graphed's partition and reduction semantics while using TaskVine for distributed
scheduling, data movement, recovery, and worker management.

The project is an early integration package. Its first objective is to keep the boundary between
graphed and TaskVine small, explicit, and testable while both projects evolve.

## Architecture

```text
                              DRIVER

  analysis code
       |
       | record once
       v
  graphed operation graph
       |
       | optimize: DCE / CSE / rewrites / stage fusion
       v
  compiled IR ----------------------------------------------------+
       |                                                          |
       | aggregate_plan                                           |
       v                                                          |
  Plan                                                            |
    process(partition, resources)  <--- shared compiled IR --------+
    tasks = [Task(key, Partition), ...]
    combine(left, right)
    empty()
       |
       | TaskVineExecutor.lower
       v
  VineGraph workflow
       |
       +-- leaf 0: process(partition 0) --+
       +-- leaf 1: process(partition 1) --+-- fixed binary combine tree --> result
       +-- leaf 2: process(partition 2) --+
       +-- ...

                              WORKER

  one leaf task
       |
       +-- read one data partition
       +-- evaluate every fused IR stage for that partition
       +-- evaluate any External nodes
       +-- return one partial result
```

There are two distinct granularities:

- graphed fuses ordinary operation subgraphs into local IR stages;
- this executor runs the complete per-partition IR as one TaskVine leaf task.

TaskVine does not receive one task per recorded array operation. For `N` fixed partitions it sees
`N` leaf tasks and `N - 1` binary combine tasks, or `2N - 1` workflow nodes. This is intentionally
coarser than a Dask graph that exposes many array, schema, and I/O keys per partition.

See [Architecture](docs/architecture.md) for component ownership and
[Executor contract](docs/executor-contract.md) for the caller-visible interface.

## Requirements

- Python 3.11 or newer;
- `graphed >= 0.0.2`;
- `graphed-executors >= 0.0.2`;
- a TaskVine build that provides `ndcctools.taskvine.vine_graph`.

TaskVine is distributed through CCTools. The standard installation route is Conda:

```bash
conda install -c conda-forge ndcctools
```

VineGraph is currently a development interface and may not be present in every CCTools release.
Verify the required surface before installing this package:

```bash
python -c "from ndcctools.taskvine.vine_graph import VineGraph, Workflow"
```

For VineGraph development, use the
[`sc26` CCTools branch](https://github.com/JinZhou5042/cctools/tree/sc26) until the interface is
available in an upstream CCTools release.

Install this repository in editable mode:

```bash
python -m pip install -e .
```

## Quick start

Build a normal graphed plan, then choose `TaskVineExecutor` at execution time:

```python
from graphed_taskvine import TaskVineExecutor

plan = build_plan()  # graphed.core.execution.Plan

with TaskVineExecutor(
    manager_name="graphed-example",
    libcores=8,
    wait_for_workers=1,
) as executor:
    result = executor.run(plan)

print(result.value)
print(executor.last_stats)
```

Workers connect to the manager in the usual TaskVine way:

```bash
vine_worker -M graphed-example
```

For a local lowering and execution check that does not require external workers:

```python
result = TaskVineExecutor(local=True).run(plan)
```

`local=True` still uses VineGraph and the same binary reduction topology; it is not the same as
graphed's `SequentialRunner`.

## Execution guarantees

- One leaf task is created for each `Plan.tasks` entry.
- Leaves are ordered by unique `Task.key` values.
- Fixed plans use the same deterministic binary tree as `graphed-executors`.
- A fixed plan with `N` partitions performs exactly `N - 1` combines.
- Worker exceptions are re-raised on the driver as their original type when serializable, with the
  remote traceback attached as an exception note.
- Relative local partition paths that exist on the driver are resolved before sandbox execution.
- Adaptive `next_tasks` plans execute one binary-reduced VineGraph workflow per round, update
  `ExecContext`, and evaluate `StopCondition` between rounds.

Task keys must be unique for the entire run. `process`, `combine`, and `empty` must follow the
graphed `Plan` contract. In particular, `combine` must be associative and commutative, and task
bodies should be safe to retry because TaskVine may re-execute work after worker loss.

## Current limitations

- The executor accepts the provisional `graphed.core.execution.Plan`; it does not yet execute
  `DurablePlanV2` shuffle or join stages.
- VineGraph task-runner calls currently fork from the worker library process. Imports are primed in
  the parent, but file handles opened through `resources.open_once` do not persist across calls.
- The complete IR for a partition runs inside one leaf task. IR stages are not separately scheduled
  across workers.
- TaskVine and the worker environment are operational prerequisites rather than Python package
  dependencies resolvable by `pip`.

The possible stage-level extension and its tradeoffs are documented in
[Stage task exploration](docs/stage-task-exploration.md).

## DV5 integration result

The initial integration was validated with a real coffea/dask-awkward HEP workflow port:

- 236 recorded graphed nodes compiled to 37 IR nodes: 35 fused stages, one source, and one
  External;
- all 236 recorded nodes remained live; the reduction came from stage fusion, not removal of the
  scientific operations;
- 4,000 input partitions lowered to 4,000 process tasks plus 3,999 binary combine tasks;
- the resulting 7,999-node VineGraph completed on 20 eight-core workers and recovered from worker
  preemption without driver intervention;
- selection and kinematic outputs matched the Dask/coffea reference; floating-point differences in
  fastjet outputs appeared only across different CPU instruction-set classes.

The corresponding Dask/coffea graph contained about 1.32 million keys before Dask optimization,
roughly 331 keys per input partition. These counts describe different scheduling granularities:
the original workflow wrote one Parquet output per partition and had no equivalent global binary
reduction, while this executor adds `N - 1` combines to produce one aggregate result.

## Development

Install test dependencies into an environment that already contains a compatible TaskVine build:

```bash
python -m pip install -e '.[test]'
ruff check .
ruff format --check .
pytest
```

The default test suite uses VineGraph local execution. Run the worker integration path with:

```bash
GTV_WORKER=1 pytest tests/test_executor.py
```

Contribution workflow and compatibility expectations are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Security boundary

Plans and worker results use `cloudpickle`. Only execute plans and accept worker connections from
trusted sources. Deserializing an untrusted plan is equivalent to executing arbitrary Python code.
See [SECURITY.md](SECURITY.md) for the supported reporting path and deployment assumptions.

## License

MIT. See [LICENSE](LICENSE).
