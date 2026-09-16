# Executor contract

## Public surface

```python
from graphed_taskvine import RunStats, TaskVineExecutor, TaskVineWorkerError
```

The supported entry point is `TaskVineExecutor.run(plan)`. `lower` is public for inspection and
benchmarking, but its returned handles belong to VineGraph.

## Constructor

```python
TaskVineExecutor(
    *,
    manager=None,
    manager_name="graphed-taskvine",
    port=(9100, 9199),
    run_info_path=None,
    run_info_template=None,
    local=False,
    libcores=16,
    wait_for_workers=0,
    work_dir=None,
    ship=(),
    params=None,
)
```

| Argument | Contract |
|---|---|
| `manager` | Optional existing `VineGraph`. The caller retains ownership. |
| `manager_name`, `port` | Used only when the executor creates a manager. |
| `run_info_path`, `run_info_template` | Forwarded to `VineGraph`. |
| `local` | Sets VineGraph `local-execute`; it does not switch to another executor. |
| `libcores` | Cores assigned to the task-runner library. |
| `wait_for_workers` | VineGraph worker-wait policy. |
| `work_dir` | Parent of executor output and checkpoint directories. |
| `ship` | Extra existing files/directories copied into the worker library sandbox. Basenames must be unique. |
| `params` | Additional VineGraph parameters. Explicit entries override adapter defaults. |

Construction is lazy: it does not bind a port or create a manager. Manager creation occurs on the
first non-empty execution or explicit access to `.manager`.

## Accepted plan

The current executor accepts `graphed.core.execution.Plan[R]`:

```python
Plan(
    process: Callable[[Partition, WorkerResources], R],
    combine: Callable[[R, R], R],
    empty: Callable[[], R],
    tasks: Sequence[Task],
    next_tasks: Callable[[ExecContext], Iterable[Task] | None] | None,
    stop: StopCondition | None,
)
```

Required invariants:

- task keys are unique for the complete run;
- `process`, `combine`, and `empty` are serializable by `cloudpickle` or import reference;
- `combine` is associative and commutative;
- `empty()` is the identity for `combine`;
- a task can be retried without corrupting external state;
- every module referenced by import is installed in the worker environment or supplied through
  `ship`.

Invalid task identity raises `ValueError` before submission. Missing shipped paths raise
`FileNotFoundError` during construction. Duplicate sandbox basenames raise `ValueError`.

## Result

`run` returns `graphed.core.execution.ExecResult[R]`:

| Field | Fixed plan | Adaptive plan |
|---|---|---|
| `value` | Root of the fixed binary tree | Fold of completed round roots |
| `n_partitions` | Number of submitted leaves | Number of completed leaves |
| `n_combines` | `max(0, N - 1)` | In-round combines plus combines between non-empty rounds |
| `stopped` | `StopReason.EXHAUSTED` | Satisfied reason, otherwise `EXHAUSTED` |

An empty fixed plan returns `empty()` without creating a TaskVine manager.

## Ordering

Fixed tasks are sorted by key. Their results are combined using the same `plan_tree` function as
the reference local executors. Completion order does not change grouping.

Adaptive batches are sorted independently. A batch is binary-reduced on workers; batch roots are
combined on the driver in round order.

## Retry and side effects

TaskVine may repeat a leaf or combine after worker loss. Receipt of a task is not proof of durable
completion. Callers should make external effects idempotent or use content-addressed output names.
The returned root proves completion of the dependency path for that run.

## Errors

| Condition | Result |
|---|---|
| Compatible TaskVine/VineGraph is unavailable | `ImportError` when manager or lowering is needed |
| Empty direct call to `lower` | `ValueError`; use `run` to obtain `empty()` |
| Duplicate task key | `ValueError` before submission |
| Missing `ship` path | `FileNotFoundError` |
| Serializable worker exception | Original exception type re-raised with remote traceback note |
| Unserializable worker exception | `TaskVineWorkerError` with traceback text |
| No root returned by VineGraph | `TaskVineWorkerError` |

## Cancellation and timeouts

The adapter does not currently expose a dedicated cancellation method. Worker lifetime, task
timeouts, and manager policies must be configured through TaskVine/VineGraph parameters. Adaptive
`StopCondition` is checked between completed rounds, not during a round.

## Compatibility policy

The package currently tracks the provisional graphed `Plan` and development VineGraph APIs. Until
both dependencies publish stable interfaces, minor releases of this package may update constructor
forwarding and compatibility shims. Changes to task ordering, reduction grouping, result counts, or
exception types require tests and a documented migration note.
