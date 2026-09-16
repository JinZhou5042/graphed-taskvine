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

## graphed vs. traditional Dask/coffea, across three workflows

Each of this repository's three examples was run both ways — through `TaskVineExecutor` and
through a plain local Dask/coffea-style pipeline doing the identical selection over the identical
input files — at three different scales: a single tiny file, a real 9.86 GB open-data download,
and a real 22 GB, 800-file HEP production dataset. The traditional side for CMS dimuon and ATLAS
H→γγ lives in this repository at [`baselines/cms_dimuon_dask.py`](baselines/cms_dimuon_dask.py)
and [`baselines/atlas_hyy_dask.py`](baselines/atlas_hyy_dask.py) (plain `uproot.dask` /
`dask.dataframe`, no coffea, no cluster — what a physicist would write without this integration).
DV5's traditional side is the original, unmodified `ecf_calculator.py` analysis from
[`JinZhou5042/sc26-dagvine-reproducibility`](https://github.com/JinZhou5042/sc26-dagvine-reproducibility)
and is not duplicated into this repository (it needs coffea, dask-awkward, and fastjet, and its
correctness depends on being byte-identical to the DAGVine/SC26 hero-run reference).

### Overview

| workflow | scale | graphed + TaskVine (wall) | traditional (wall) | speedup | correctness |
|---|---|---|---|---|---|
| [CMS dimuon](#quick-start) | 1 file, 2,304 events (toy) | 1.79 s | 1.77–1.89 s | **~1x (noise)** | exact match |
| [ATLAS H→γγ](#quick-start) | 16 files, 9.86 GB | 9.9 s (5.35 s execution) | 52.25 s | **~5.3x** | exact match |
| [DV5](#dv5-run-it-yourself) | 800 files, 22 GB | 499.7 s (~8.3 min) | 1,316.1 s (~21.9 min) | **~2.6x** | bit-for-bit identical |

All three ran locally, single machine, no distributed factory. "Traditional" always means the
same physics, computed without graphed or TaskVine. Details for each workflow follow.

### CMS dimuon (toy scale)

Single 2,304-event ROOT file, opposite-sign muon pairs, invariant mass, 60-bin 60–120 GeV
histogram — [`examples/cms_dimuon.py`](examples/cms_dimuon.py) vs.
[`baselines/cms_dimuon_dask.py`](baselines/cms_dimuon_dask.py) (`uproot.dask` + plain
`dask.compute`, 5 partitions to match graphed's `steps_per_file=5`):

| | graphed + TaskVine | traditional dask-awkward |
|---|---|---|
| events in 60–120 GeV | 2004 | 2004 |
| peak bin center | 90.5 GeV | 90.5 GeV |
| scheduled units | 9 VineGraph tasks (5 process + 4 combine) | 135 dask graph keys |
| cold wall time | 1.789 s | 1.77–1.89 s |

**No meaningful speedup at this scale, and that's the finding.** Real work inside the script is
well under a second on both sides (graphed's own makespan is 0.107 s; the dask baseline's
build+compute is 0.46–0.66 s) — the ~1.8 s each process actually takes is dominated by Python and
library import time, not by either framework's scheduling. A single small file is exactly the
regime where this integration has nothing to offer over calling `uproot`/`awkward` directly; it
exists here as a fast, low-dependency smoke test, not a performance argument.

### ATLAS H→γγ (medium scale, real Open Data)

16 real ATLAS Open Data ROOT files (~9.86 GB), diphoton selection and mass reconstruction, 60-bin
100–160 GeV histogram — [`examples/atlas_hyy.py`](examples/atlas_hyy.py) vs.
[`baselines/atlas_hyy_dask.py`](baselines/atlas_hyy_dask.py) (`dask.dataframe.read_parquet` over
the same per-file flat Parquets, `scheduler="processes"`, 4 workers, matching graphed's
`libcores=4`):

| | graphed + TaskVine | traditional Dask |
|---|---|---|
| events in 100–160 GeV | 251659 | 251659 |
| peak bin center | 100.5 GeV | 100.5 GeV |
| scheduled units | 31 VineGraph tasks (16 process + 15 combine) | 1,232 dask graph keys |
| graph build | — | 0.13 s |
| execution / compute | 5.35 s makespan | 50.79 s |
| **total wall time** | **9.9 s** | **52.25 s** |

**~5.3x faster wall-to-wall, with no metadata-scan trick involved** — both sides read the same
already-flattened Parquet files, so this gap is scheduling granularity alone: one VineGraph leaf
task per file running the whole fused IR vs. Dask spreading the same work over 1,232 keys, each
with its own scheduling overhead. This is the cleanest read of graphed's per-partition coarsening
in this repository, since there's no expensive `uproot` metadata scan here to muddy the picture
(unlike DV5, below).

### DV5 (large scale, real HEP production workload)

DV5 is the ECF-calculator H→γγ PFNano skim behind the DAGVine/SC26 hero run: a real ATLAS
diphoton analysis (softdrop-fix event cut, trigger OR, lepton/tau counting with ΔR cleaning,
b-tag counting, generator-level Higgs matching, the fat-jet selection, and per-jet substructure —
color ring and energy correlation functions via `fastjet`). [`examples/dv5.py`](examples/dv5.py)
records the whole selection above in graphed; jet substructure (PF constituents → fastjet C/A →
soft drop → ECFs + color ring) is one **External** node, because fastjet cannot run on awkward
typetracers, so graphed takes its output form from running it once on a tiny synthetic event. No
coffea is used on the graphed side — NanoEvents schema behavior is replaced by explicit column
access and the same vector formulas coffea uses. The 22 GB `hgg_0` input dataset (800 ROOT files,
one `--copy-count 1` copy of the DAGVine reproducibility archive) comes from the reproducibility
repository cited above.

<a id="dv5-run-it-yourself"></a>Run it yourself (this needs `fastjet`, `vector`, `scipy`, and the
`uproot.graphed`-providing `graphed-org/uproot5-graphed-mvp` development fork in place of released
`uproot` -- see the module docstring):

```bash
python -m examples.dv5 --data-dir dv5-data --files 5
```

By default this downloads the ~22 GB Zenodo archive on first use and processes the first 5 of its
800 files; point `--data-dir` at an existing extraction (for example a previously downloaded
`hgg_0`) and pass `--files 0` to reproduce the full-dataset numbers below with local execution.
`--distributed` is not yet verified for this example (unlike `examples/atlas_hyy.py`); the numbers
below were produced with the underlying analysis run directly against a real `vine_worker`, not
through this CLI's `--distributed` flag.

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

### Key insights, across all three

- **Speedup tracks how much real work there is, not raw file count.** Toy scale (1 file): no
  signal, overhead-dominated, ~1x. Medium scale (16 files, real compute, no metadata bottleneck):
  ~5.3x from scheduling granularity alone. Large scale (800 files, expensive per-file metadata):
  ~2.6x, with the metadata scan itself the single biggest lever.
- **The dominant cost on the traditional side, once files are numerous and metadata is expensive,
  is opening files, not computing.** graphed's blind partitions never open the 799 non-schema DV5
  files; Dask/coffea's `apply_to_fileset` needs a `steps`/`num_entries` fileset and paid a 684 s
  single-threaded scan to build one here. A real coffea pipeline normally amortizes this with a
  cached, pre-built fileset (what `samples_ready.json` represents) — but that cache still has to be
  built once, and wasn't available for the full 22 GB set in this run.
- **graphed is still faster even ignoring the scan, at both the medium and large scale.** DV5:
  495.4 s of VineGraph execution over 1,599 scheduled units beat 629.3 s of Dask compute over
  264,826 graph keys. ATLAS H→γγ: 5.35 s over 31 units beat 50.79 s over 1,232 keys, with no scan
  cost on either side. Both are consistent with the same mechanism — one leaf task runs all fused
  IR stages for a partition, instead of exposing every array/schema/IO operation as a separate
  scheduled key.
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
- **Correctness held exactly at every scale.** 2004/2004 (dimuon), 251659/251659 (H→γγ), and
  275/275 with 39/39 bit-for-bit leaves (DV5) — the speedups above come from scheduling, not from
  cutting corners on the physics.
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
