# Changelog

## 0.1.0

- Add `TaskVineExecutor` for fixed and adaptive graphed plans.
- Lower one process task per partition and a deterministic binary combine tree.
- Preserve serializable worker exception types and attach remote tracebacks.
- Support VineGraph local execution and externally managed managers.
- Resolve existing relative local partition paths before sandbox execution.
- Reject duplicate task keys and colliding shipped sandbox destinations.
- Expose lowering and execution statistics through `RunStats`.
