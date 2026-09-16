# graphed-taskvine

`graphed-taskvine` runs a [`graphed`](https://github.com/graphed-org/graphed) `Plan` on
[TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) through the VineGraph interface.
It keeps graphed's partition and reduction semantics while using TaskVine for distributed
scheduling, data movement, recovery, and worker management.

This repository contains the integration layer; it is not intended to become a separately
published package. Its first objective is to keep the boundary between graphed and TaskVine small,
explicit, and testable while both projects evolve.

The source boundary is deliberately flat:

```text
taskvine_backend.py       stable driver-side interface
_task_runtime.py          private worker-side process/combine bodies
_vinegraph_context.py     private VineGraph library initialization
```

There is no repository-named Python package or duplicated graphed source tree.

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
Verify the required surface before using this integration:

```bash
python -c "from ndcctools.taskvine.vine_graph import VineGraph, Workflow"
```

For VineGraph development, use the
[`sc26` CCTools branch](https://github.com/JinZhou5042/cctools/tree/sc26) until the interface is
available in an upstream CCTools release.

Clone the repository, install its development dependencies, and run from the repository root:

```bash
python -m pip install -r requirements.txt
```

## Quick start

Build a normal graphed plan, then choose `TaskVineExecutor` at execution time:

```python
from taskvine_backend import TaskVineExecutor

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

## Integration interface

```python
TaskVineExecutor(
    manager=None,                 # optional caller-owned VineGraph manager
    manager_name="graphed-taskvine",
    port=(9100, 9199),
    local=False,                  # VineGraph local-execute mode
    libcores=16,
    wait_for_workers=0,
    work_dir=None,
    ship=(),                      # extra worker-side modules
    params=None,                  # additional VineGraph parameters
)
```

The executor accepts `graphed.core.execution.Plan` and returns
`graphed.core.execution.ExecResult`. Construction is lazy: the manager is created only when a
non-empty plan runs or `.manager` is accessed. A caller-supplied manager remains caller-owned;
otherwise `close()` or the context manager releases the executor-owned manager.

The stable interface is `TaskVineExecutor.run(plan) -> ExecResult`, the lifecycle methods
`close()`, `__enter__()`, and `__exit__()`, `last_stats`, `RunStats`, and
`TaskVineWorkerError`. It directly implements graphed's existing `Executor` protocol; this
repository does not define a competing executor interface.

`lower(plan)` remains available as an advanced graph-inspection and benchmarking hook, but it is
not part of the compatibility contract. It returns the VineGraph workflow and root handle, and
requires at least one task. Missing shipped paths, duplicate sandbox destinations, and duplicate
task keys fail before submission. Worker failures preserve their original exception type when
serializable; otherwise `TaskVineWorkerError` carries the remote traceback.

Plans and shipped modules are trusted executable inputs. TaskVine may retry leaves or combines, so
external writes must be idempotent or content-addressed. Adaptive stop conditions are evaluated
between rounds rather than during a running round.

## Current limitations

- The executor accepts the provisional `graphed.core.execution.Plan`; it does not yet execute
  `DurablePlanV2` shuffle or join stages.
- VineGraph task-runner calls currently fork from the worker library process. Imports are primed in
  the parent, but file handles opened through `resources.open_once` do not persist across calls.
- The complete IR for a partition runs inside one leaf task. IR stages are not separately scheduled
  across workers.
- TaskVine and the worker environment are operational prerequisites rather than Python package
  dependencies resolvable by `pip`.

Scheduling individual fused stages is technically possible but is not the default direction. It
would require stage-addressable evaluation, durable intermediate schemas, worker affinity, and
intermediate transfer/cache ownership. For DV5, expanding 35 stages plus one External across 4,000
partitions would create roughly 144,000 computation tasks before result reduction. A future
extension should therefore introduce optional coarse boundaries around measured expensive
Externals, resource transitions, or checkpoints instead of creating one remote task per stage.

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

In an environment that already contains a compatible TaskVine build:

```bash
python -m pip install -r requirements.txt
ruff check --target-version py311 --line-length 110 --select E,F,I,UP,B,SIM,C4,RUF --ignore E501 *.py tests examples
ruff format --check --target-version py311 --line-length 110 *.py tests examples
python -m pytest -q tests
```

The default test suite uses VineGraph local execution. Run the worker integration path with:

```bash
GTV_WORKER=1 python -m pytest -q tests/test_executor.py
```

## Security boundary

Plans and worker results use `cloudpickle`. Only execute plans and accept worker connections from
trusted sources. Deserializing an untrusted plan is equivalent to executing arbitrary Python code.
The adapter delegates worker authentication, network policy, and filesystem isolation to the
TaskVine deployment. Do not embed credentials in plans, partition URIs, logs, or shipped modules.

## License

MIT. See [LICENSE](LICENSE).
