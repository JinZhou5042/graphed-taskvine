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

## Install VineGraph

VineGraph is currently a CCTools development interface, so the released `ndcctools` Conda package
may not include it. Build the `task-graph` branch in its own Conda environment:

```bash
git clone --branch task-graph --single-branch https://github.com/JinZhou5042/cctools.git cctools-src
cd cctools-src
unset PYTHONPATH
conda env create -y -f environment.yml
conda install -y -n cctools-dev --override-channels -c conda-forge --strict-channel-priority python=3.13
conda activate cctools-dev
./configure --with-base-dir "$CONDA_PREFIX" --prefix "$CONDA_PREFIX"
make -j4
make install
```

Verify both the Python interface and worker executable:

```bash
python -c "from ndcctools.taskvine.vine_graph import VineGraph, Workflow"
vine_worker --version
```

The branch's
[VineGraph guide](https://github.com/JinZhou5042/cctools/blob/task-graph/doc/manuals/taskvine/vine-graph.md)
covers local workflows, workers, HTCondor submission, factories, and execution parameters. The
explicit Python pin above avoids an untested future Python version when `environment.yml` resolves
its broad `python=3` requirement.

## Install Graphed and this integration

For an existing Python environment, Graphed's Awkward and Parquet support is installed with
`python -m pip install "graphed[awkward,parquet]"`. For this integration, clone the repository
inside the active `cctools-dev` environment. Its requirements also install the Graphed executor
and histogram libraries, Uproot, and the small set of development dependencies used here:

```bash
git clone https://github.com/JinZhou5042/graphed-taskvine.git
cd graphed-taskvine
python -m pip install -r requirements.txt
python -c "import graphed; from taskvine_backend import TaskVineExecutor"
```

## Quick start

The example reconstructs the H -> gamma gamma diphoton mass spectrum from the 16 public GamGam ROOT
files of the `2025e-13tev-beta` [ATLAS Open Data](https://opendata.cern.ch) 13 TeV release (about
9.86 GB). It downloads the files through `atlasopenmagic`/`fsspec`, keeps the leading two photons per
event, records the same selection as the ATLAS Open Data H->yy notebook (tight photon ID, pT,
calorimeter isolation, eta transition-region veto, and the diphoton invariant mass) with Graphed, and
fills a 100-160 GeV histogram through this executor. One partition is created per input ROOT file.

Run the complete workflow locally through VineGraph:

```bash
python -m examples.atlas_hyy
```

Expected summary (from an actual run against the full 16-file dataset):

```text
events in 100-160 GeV: 251659
highest bin center: 100.5 GeV
process/combine tasks: 16/15
```

The highest bin sits at the low edge of the window because the diphoton background falls roughly
monotonically over 100-160 GeV; the H -> gamma gamma signal is a small excess near 125 GeV on top of
that background, not the tallest bin, at this sample size.

The core analysis in [`examples/atlas_hyy.py`](examples/atlas_hyy.py) is ordinary deferred Graphed
code operating on the flattened leading-two-photon columns:

```python
import graphed_histogram as gh
from graphed import Session
from graphed.awkward import AwkwardBackend, from_parquet

from taskvine_backend import TaskVineExecutor

session = Session(AwkwardBackend())
events = from_parquet(session, "events", [str(p) for p in parquet_paths])  # one file per partition

tight = events.isTightID_0 & events.isTightID_1
pt_cut = (events.pt_0 > 50) & (events.pt_1 > 30)
isolation = (events.ptcone20_0 / events.pt_0 < 0.055) & (events.ptcone20_1 / events.pt_1 < 0.055)
eta_ok = in_transition_veto(events.eta_0) & in_transition_veto(events.eta_1)
selected = events[tight & pt_cut & isolation & eta_ok]

mass = diphoton_mass(selected)  # by-hand 4-vector algebra, same formula as examples/cms_dimuon.py
histogram = gh.boost.Histogram(bh.axis.Regular(60, 100.0, 160.0), storage=bh.storage.Int64())
histogram.fill(mass)
plan = gh.plan({"diphoton_mass": histogram})

with TaskVineExecutor(local=True, port=0) as executor:
    result = executor.run(plan)
```

To execute the same plan on a TaskVine worker, use two terminals in the same environment:

```bash
# terminal 1
vine_worker -M graphed-hyy --cores 4

# terminal 2, from this repository
python -m examples.atlas_hyy --distributed
```

`local=True` still uses VineGraph and the same binary reduction topology; it is not the same as
graphed's `SequentialRunner`.

[`examples/cms_dimuon.py`](examples/cms_dimuon.py) remains in the repository as a much smaller and
faster (single-file, 2,304-event) smoke test of the same pattern.

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

## graphed vs. traditional Dask/coffea: the DV5 workflow

DV5 is the ECF-calculator H→γγ PFNano skim behind the DAGVine/SC26 hero run: a real ATLAS
diphoton analysis (softdrop-fix event cut, trigger OR, lepton/tau counting with ΔR cleaning,
b-tag counting, generator-level Higgs matching, the fat-jet selection, and per-jet substructure —
color ring and energy correlation functions via `fastjet`). `dv5_graphed.py` records the whole
selection above in graphed; jet substructure (PF constituents → fastjet C/A → soft drop → ECFs +
color ring) is one **External** node, because fastjet cannot run on awkward typetracers, so
graphed takes its output form from running it once on a tiny synthetic event. No coffea is used
on the graphed side — NanoEvents schema behavior is replaced by explicit column access and the
same vector formulas coffea uses. The original, unmodified Dask/coffea analysis and the 22 GB
`hgg_0` input dataset (800 ROOT files, one `--copy-count 1` copy of the DAGVine reproducibility
archive) come from
[`JinZhou5042/sc26-dagvine-reproducibility`](https://github.com/JinZhou5042/sc26-dagvine-reproducibility).

### Key results

Both systems ran the full 22 GB, 800-file `hgg_0` dataset locally on the same machine, capped at
16 cores (graphed: one local `vine_worker --cores 16`; Dask/coffea: `dask.compute(..., scheduler=
"processes", num_workers=16)`), with no `samples_ready.json` cache available for either side:

| stage | graphed + TaskVine | traditional Dask/coffea |
|---|---|---|
| fileset metadata scan | ~0 s (blind partitions: one file opened for schema, `N` for data) | 684.4 s (0.86 s/file × 800, single-threaded `uproot.open`) |
| graph build | 2.19 s (`source_open` 1.02 s + `record` 1.16 s + `compile` 0.008 s) | 2.4 s (`apply_to_fileset`) + 0.003 s (key count) |
| scheduled units | 1,599 VineGraph tasks (800 leaves + 799 combines) | 264,826 graph keys (331/file) |
| execution / compute | 495.4 s makespan (real local worker, 16 cores) | 629.3 s (`scheduler=processes`, 16 workers) |
| **total wall time** | **≈ 499.7 s (~8.3 min)** | **≈ 1,316.1 s (~21.9 min)** |
| selected events (of 800 files) | 275 | 275 |

Correctness: **bit-for-bit identical**. Both sides selected the same 275 of 275 events, and all 39
compared leaves (`Color_Ring`, 32 ECFs, `msoftdrop`, `pt`, `btag_ak4s`, `pn_HbbvsQCD`, `pn_md`,
`matching`) matched with `max_rel_diff = 0` (`compare.py`) — both ran on the same CPU this time, so
there is none of the cross-microarchitecture fastjet drift seen in the multi-machine condor runs
described below.

### Key insights

- **The dominant cost on the traditional side is metadata, not compute.** graphed's blind
  partitions never open the 799 non-schema files; Dask/coffea's `apply_to_fileset` needs a
  `steps`/`num_entries` fileset and paid a 684 s single-threaded scan to build one here. A real
  coffea pipeline normally amortizes this with a cached, pre-built fileset (what
  `samples_ready.json` represents) — but that cache still has to be built once, and wasn't
  available for the full 22 GB set in this run.
- **graphed is still faster even ignoring the scan.** 495.4 s of VineGraph execution over 1,599
  scheduled units beat 629.3 s of Dask compute over 264,826 graph keys for the identical physics —
  consistent with the coarser per-partition granularity (one leaf task runs all 35 fused IR stages
  instead of exposing every array/schema/IO operation as a separate key).
- **The metadata scan is single-threaded by construction.** `run_dask_reference.py`'s fallback
  scan is a plain per-file Python loop, not parallelized across the 16-core cap used for
  `compute()`; a more engineered pipeline could parallelize or cache it, but this is what the
  unmodified reference script does.
- **The External boundary is where correctness risk concentrates.** Jet substructure can't run on
  typetracers, so it's one opaque node whose correctness depends on faithfully mirroring the
  original kernel. One concrete footgun found while validating this port: fastjet's dask-awkward
  wrapper defaults `exclusive_jets_energy_correlator` to `normalized=False`, while the eager API
  defaults to `normalized=True` — a naive eager port is off by roughly `pT^n` (reference ~1e4,
  eager ~0.05) unless `normalized=False` is passed explicitly, which both sides do here.
- **This holds up at larger scale, on real distributed workers, with real failures.** A separate
  HTCondor run (`800` and `4,000` files, `10×8`- and `20×8`-core workers) produced a 236-node
  recorded program compiling to the same 37 IR nodes (35 fused stages, one source, one External)
  regardless of input size, lowered to 1,599- and 7,999-node VineGraphs; the 4,000-file run
  survived a full first-wave HTCondor preemption (22 worker connections/disconnections, ~4,830
  re-executions of lost intermediates) with no driver intervention, and matched the Dask/coffea
  reference to exact selection/kinematics with fastjet outputs agreeing to ≤ 1.03e-5 relative
  (attributable to CPU instruction-set differences across machines, not the framework). The
  corresponding Dask/coffea graph reached about 1.32 million keys before optimization at 4,000
  files (~331 keys per input partition) against `2N − 1` graphed tasks.

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

## License

MIT. See [LICENSE](LICENSE).
