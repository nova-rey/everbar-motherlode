# GPU feasibility review — 2026-08-22

## Decision

**Do not introduce a GPU implementation into the active Motherlode ingestion
path.**  The observed hot path is per-stream MIDI parsing, stateful
performance flattening, and the pinned Everbar Brick 3 subprocess.  It is not
a dataframe, text, tokenization, or regular-array workload.  A CUDA/JAX/RAPIDS
rewrite would be high-risk and has no demonstrated end-to-end payoff.

This is a profiling conclusion, not a claim that GPUs are never useful in this
project.  A later, independent canonical feature or musical-fingerprint pass
may have a regular numeric representation worth batching on a GPU.

## Scope and measurement conditions

The production workload examined was the active PDMX derivation wave.  The
worker receives pre-extracted MIDI files, creates a one-track V1 candidate,
runs `performance-flattening-v1`, invokes the exact pinned
`everbar-inspect-midi` Brick 3 authority, and persists a SQLite/JSON receipt.

| Environment | Result |
| --- | --- |
| Active Lightning worker | 4 vCPU, 15 GiB RAM; no `nvidia-smi`/CUDA device; no cuDF, CuPy, JAX, Torch, pandas, Arrow, or Polars installed. |
| Local development host | 4 logical CPU, 3.8 GiB RAM; no NVIDIA device. |
| Real PDMX progress sample | 19,206 converted streams, 3.339 streams/s aggregate, with three PDMX workers active and a fourth CPU occupied by a separate MAESTRO/Aria worker. |
| Brick 3 direct invocation sample | 0.369 s for one real pre-Brick-3 MIDI candidate; the `uv run` wrapper took 0.390 s. |

The live process sample showed four simultaneous `everbar-inspect-midi`
children collectively consuming most available CPU.  Its PDMX supervisor
processes used roughly 4–5% CPU each.  This is strong evidence that the
per-candidate Brick 3 child is the compute-heavy active stage.

The aggregate rate implies roughly 0.90 worker CPU-seconds per converted stream
under that load.  The direct Brick 3 timing is an **individual-file sample**,
not a global average; it establishes a lower-bound-sized, material part of the
per-stream work but is not used to claim an exact stage percentage.  We did
not run intrusive instrumentation on the saturated production worker merely to
obtain a more granular profile.

## Pipeline breakdown

Motherlode does **not** currently process JSONL/text records or DataFrames. It
uses Python's `urllib`, `zipfile`/`tarfile`, `mido`, SQLite, JSON receipts, and
the pinned external Brick 3 CLI.  It does not run a tokenizer or PerTok encoder
in the Motherlode process.

| Stage | Evidence / observed time | Main limiting resource | Implementation | GPU class | Conclusion |
| --- | --- | --- | --- | --- | --- |
| Download / remote transfer | Not active during the sampled derivation wave; artifacts were already local. | Network | `urllib` / `rclone` at distribution boundary | E | GPU cannot remove network wait. |
| Archive extraction | One-time, pre-derivation; Python `zipfile`/`tarfile`; no measured evidence it dominates the active run. | Disk + CPU | Python stdlib/native decompression | D/E | Prefer parallel CPU compression tooling only if a future measurement proves this material. |
| MIDI read and track inventory | Per source MIDI, dynamic message objects and variable tracks. | CPU / Python object allocation | `mido` | D | Branchy MIDI event parsing has poor GPU transfer and implementation economics. |
| Candidate write and flattening | Per-track state machine: note pairing, CC64/CC66 lifetimes, CC67 receipt, CC121 reset semantics. | CPU / Python control | `mido` + Motherlode | D | Exact ordering/state semantics make a bulk GPU rewrite risky. |
| Brick 3 inspection | Direct sample: 0.369 s/candidate; live child processes occupy most CPU. | CPU | pinned Everbar CLI, Python + existing upstream stack | D | It remains final semantic authority and has no CUDA boundary. |
| SQLite/JSON receipts | Durable per-candidate metadata; no sign of material CPU use in live process sample. | Small I/O | SQLite / JSON | D | Keep on CPU. |
| Partition discovery | Previously repeated tree/allow-list walks; now cached durable deterministic manifest. | Disk + Python | Motherlode | D | CPU optimization already implemented without semantic change. |
| Dedupe/fingerprint / future feature batches | Not the sampled active bottleneck.  Canonical event data can form numeric batches later. | Unknown until measured | planned / separate derived pass | B | Potential later target, not a reason to alter ingestion. |

No current stage meets the evidence threshold for class A (directly
GPU-friendly) or for a safe near-zero-rewrite GPU acceleration.

## CPU improvements already justified

The following changes preserve the source-to-Brick-3 contract and are better
aligned with the observed workload than GPU work:

1. Cache the deterministic PDMX partition manifest instead of re-walking the
   full source tree and rebuilding the allow-list in every shard.
2. Reuse a raw source hash and track note inventory within a source piece.
3. Use four resumable, non-overlapping CPU slots, but avoid oversubscribing the
   four-vCPU Lightning host during handoff from the MAESTRO/Aria worker.
4. Launch the next GigaMIDI wave from the completion boundary rather than
   requiring an interactive handoff.

The first three are already in the deployed source revision; their exact
end-to-end speedup must be measured after the between-chunk handoff completes.
The `uv` wrapper experiment suggests replacing it alone would save only about
5% of that isolated Brick 3 invocation and is not a worthwhile semantic or
operational change by itself.

## RAPIDS/cuDF result

cuDF is not applicable to the active path: there is no pandas/DataFrame, CSV,
JSONL, Arrow, or bulk string-column hot path to accelerate.  Installing RAPIDS
on a GPU host would add a large dependency without placing the dominant MIDI or
Brick 3 computation on the device.  Therefore no `cudf.pandas` run was made.

If a future tabular metadata-overlay/fingerprint job is profiled as material,
start with cuDF's pandas accelerator and its fallback profiler, not a rewrite.
cuDF documents `python -m cudf.pandas` and explicitly recommends profiling
CPU fallbacks/transfer boundaries before judging a result.

References: [cuDF pandas accelerator usage](https://docs.rapids.ai/api/cudf/stable/cudf_pandas/usage/)
and [cuDF pandas FAQ](https://docs.rapids.ai/api/cudf/stable/cudf_pandas/faq/).

## Compression and tokenization

GPU compression/decompression is not justified by the active measurements.
Extraction is a one-time CPU/I/O preparation phase, not the compute-heavy
per-stream stage.  If archive throughput later becomes measurable as a large
share, compare CPU `pigz`/parallel-zstd settings before nvCOMP; only adopt a GPU
path if it improves the whole ingest-to-verified-output pipeline.

Motherlode does not tokenize input text or call a PerTok tokenizer.  Brick 3 is
the upstream semantic authority.  Consequently there is no tokenizer whose
token IDs could be replaced or GPU-accelerated in this repository.

## JAX/XLA feasibility

**Verdict: POOR FIT for current ingestion.**  JAX's natural unit is an array,
while the active work consists of file I/O, variable-length MIDI messages,
stateful controller ordering, subprocess policy execution, and durable receipt
writes.  Flattening this into tensors would add host/device copies and complex
semantic edge cases without removing Brick 3 from the critical path.

JAX's array-oriented type model is documented in its
[array/type documentation](https://docs.jax.dev/en/latest/jax.typing.html).

There is a potential future boundary after canonical persistence:

```
canonical event rows -> fixed-width note/shingle batches -> reductions or minhash -> derived features
```

That boundary is independent of ingestion and can be evaluated later with an
exact CPU-vs-GPU equivalence fixture.  It must never alter canonical IDs,
Brick-3 results, or source provenance.

## Recommended next experiment

Do not prototype GPU MIDI ingestion.  When a real CUDA host is available,
benchmark a **separate canonical-event fingerprint or primitive-feature batch**
using one fixed input Parquet/SQLite export and exactly compare its output to a
CPU reference.  Measure batches of 64, 256, 1,024, 4,096, and 16,384 rows or
streams as memory permits, including transfer time and warm-up separately.

Acceptance gate:

- identical IDs, values, and ordering where ordered;
- no raw MIDI, Brick 3, or corpus-state mutation;
- a meaningful end-to-end improvement after CPU-to-GPU and GPU-to-CPU transfer;
- a measured win large enough to justify an optional dependency.

Until that exists, invest economical compute in independent CPU shards.  There
is no defensible GPU full-corpus ETA or dollar estimate yet: no GPU is available
to benchmark, no GPU-compatible active bottleneck was found, and a price point
was not selected.  Reporting one would be fabricated precision.

## Reproducibility notes

This review deliberately made no semantic pipeline change, did not interrupt
the active run, did not add GPU dependencies, and did not modify Brick 3.
Future measurements should record GPU model/VRAM, driver and library versions,
CPU/RAM, storage type, source sample identity, cache state, warm-up time, peak
RAM/VRAM, host-device transfer time, and exact equivalence results.
