# Stage-level distributed tasks

## Current boundary

Today the distributed scheduling unit is one data partition:

```text
(partition, complete compiled IR) -> one TaskVine leaf task
```

Inside that task, the graphed evaluator walks the reduced IR and executes its fused stages locally.
This keeps intermediate Awkward arrays in one process and exposes only partition results to the
distributed scheduler.

## Possible extension

A future executor could make selected IR regions addressable:

```text
(partition, stage group 0) -> intermediate 0
(partition, stage group 1, intermediate 0) -> intermediate 1
(partition, External, intermediate 1) -> partial result
```

This is feasible but requires contracts that do not exist yet:

1. an evaluator entry point for one stage or a declared stage group;
2. stable names and schemas for intermediate values;
3. serialization, transfer, spill, and cache ownership for intermediates;
4. dependency lowering for fan-out and shared stage outputs;
5. worker-affinity and resource hints;
6. stage-level retry and checkpoint semantics;
7. error attribution that preserves graphed source frames.

`DurablePlanV2` demonstrates that graphed can describe true multi-stage shuffle and join plans, but
it does not make every ordinary IR stage independently executable.

## Cost model

Splitting can help when a region:

- dominates partition runtime;
- needs a specialized resource such as a GPU;
- produces a small reusable result;
- benefits materially from independent retry or checkpointing;
- leaves worker cores idle because partition-level parallelism is insufficient.

Splitting can hurt when it:

- turns short local operations into scheduler events;
- serializes large jagged intermediates;
- moves data away from the worker that read the source file;
- duplicates fan-out values;
- loses process-local caches.

For the DV5 case study, the compiled IR contains 35 fused stages and one External. Expanding every
computational node for 4,000 partitions would create about 144,000 stage/External tasks before the
3,999 result-combine tasks. If source reads were also separate tasks, the count would be about
148,000 before result combination.

## Recommended evolution

Do not make every fused stage a remote task by default. Add an optional macro-stage boundary that
can be selected by:

- an explicit user annotation;
- an External or checkpoint boundary;
- a resource transition;
- a measured cost and intermediate-size model.

The default remains one partition task. A split plan must demonstrate that added parallelism,
resource placement, or recovery value exceeds scheduling and intermediate-transfer cost.

## Evaluation criteria

A prototype should report, for both whole-IR and split execution:

- scheduler-visible task count;
- manager CPU and graph-lowering time;
- intermediate bytes serialized, transferred, and spilled;
- cache hit rate and worker locality;
- makespan and total CPU time;
- retry work after an injected worker loss;
- bitwise or tolerance-based result agreement.

The first useful experiment is a single explicit boundary around an expensive External, not a
general one-task-per-stage expansion.
