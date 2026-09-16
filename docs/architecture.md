# Architecture

## Component boundary

```text
 +--------------------------+        owns recording and IR semantics
 | graphed                  |
 |                          |
 | Session / Arrays         |
 | optimizer / compiled IR  |
 | Plan / Task / Partition  |
 +-------------+------------+
               |
               | Plan contract
               v
 +-------------+------------+        owns lowering and semantic adaptation
 | graphed-taskvine         |
 |                          |
 | TaskVineExecutor         |
 | worker call wrappers     |
 | error/result transport   |
 +-------------+------------+
               |
               | Workflow + function calls
               v
 +-------------+------------+        owns scheduling and recovery
 | TaskVine / VineGraph     |
 |                          |
 | manager / workers        |
 | sandbox / data transfer  |
 | retry / recovery         |
 +--------------------------+
```

This repository is an adapter. It does not own graphed's optimizer or TaskVine's scheduler.

## Compile once, execute per partition

`graphed.aggregate_plan` compiles the recorded analysis once. The resulting `Plan.process` carries
the shared compiled IR, while `Plan.tasks` carries input partitions:

```text
                         one compiled IR
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
        partition 0      partition 1      partition 2
              |                |                |
              v                v                v
          leaf task        leaf task        leaf task
```

A leaf task calls:

```python
plan.process(task.partition, worker_resources)
```

The full IR is evaluated inside that call. Fused IR stages are local evaluation units, not
distributed TaskVine tasks.

## Lowering a fixed plan

For a fixed set of tasks, lowering has two steps:

1. Sort leaves by `Task.key` and create one VineGraph task per partition.
2. Use `graphed_executors.local.plan_tree` to create the fixed binary combine tree.

For seven leaves, the logical topology is:

```text
 level 0:   0       1       2       3       4       5       6
             \     /         \     /         \     /        |
 level 1:      7               8               9             6
                 \           /                   \         /
 level 2:            10                              11
                         \                         /
 level 3:                         12
```

An unpaired node carries to the next level unchanged. The tree has `N - 1` combine nodes and depth
`ceil(log2(N))`. A combine becomes runnable as soon as both inputs complete; there is no global
barrier before reduction. Fixed leaf ordering gives deterministic left/right grouping independent
of completion order.

## Worker transport

The executor serializes `(process, combine, empty)` once with `cloudpickle`. The bytes are an opaque
leaf value in VineGraph arguments so the graph walker does not interpret embedded callables.

The task-runner library:

1. adds its sandbox to `sys.path`;
2. imports shipped modules;
3. primes distinct plan blobs in the library parent process;
4. forks calls that execute leaf or combine functions.

`graphed_taskvine` is always shipped. Additional modules named by import reference must be listed in
`TaskVineExecutor(ship=[...])`. Two shipped paths may not use the same destination basename.

## Errors

A leaf or combine failure is represented by a partial containing:

- a serialized exception when possible;
- a formatted remote traceback;
- the task key, or `"combine"` for a reduction failure.

Failure partials short-circuit through the combine tree. The driver re-raises a deserialized
exception with the remote traceback attached. If the exception itself cannot be serialized,
`TaskVineWorkerError` carries the text traceback.

## Adaptive plans

An adaptive plan does not know its complete partition set in advance. Each `next_tasks(ctx)` batch
is lowered to its own fixed binary tree. After a round:

```text
round result
    + task durations ------> ExecContext.last_durations
    + processed entries ---> ExecContext.events_done
    + elapsed time --------> ExecContext.elapsed_s
```

`StopCondition` is evaluated between rounds. Round results are folded in submission order. Adaptive
execution therefore preserves associative/commutative reduction semantics but does not promise the
same floating-point grouping as a fixed plan.

## Resource lifecycle

- The VineGraph manager is created lazily.
- A manager supplied by the caller is never closed by this executor.
- A manager created by the executor is released by `close()` or the context manager.
- Output and checkpoint directories are created immediately before execution.
- Task-runner calls currently fork, so `open_once` handles opened during one call do not persist to
  the next call.

## Trust boundary

Plan blobs are executable Python payloads. Driver, workers, shipped modules, and cached task results
must belong to the same trust domain. This adapter does not authenticate or sandbox Python code; it
delegates worker connectivity and filesystem isolation to TaskVine.
