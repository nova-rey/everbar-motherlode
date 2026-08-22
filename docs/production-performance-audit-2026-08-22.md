# Production performance audit — 2026-08-22

## Scope and invariants

This audit concerns the active raw-MIDI → V1 stream →
`performance-flattening-v1` → pinned Everbar Brick 3 flow. Every improvement
below preserves raw bytes, candidate IDs and output paths, partition ownership,
conversion receipts, Brick 3 as authority, and V2 provenance. It intentionally
does not change a corpus policy, canonical identity, or acceptance rule.

The active Lightning workload is CPU-bound in separate Brick 3 child processes.
The parent workers are not a throughput bottleneck by themselves, so adding
unbounded inner multiprocessing would simply oversubscribe the host. The
existing four independent, resumable outer workers are the correct main unit of
parallelism.

## Applied improvements

| Change | Why it matters | Semantic protection |
| --- | --- | --- |
| Durable GigaMIDI partition manifest | Without it, every Giga worker walks and sorts the complete extracted tree before retaining its partition. The first worker creates a lock-protected map; all other workers consume their explicit path list. | Uses the pre-existing sorted discovery result and `partition_for()` assignment exactly. |
| Single-pass source inventory | Raw source bytes are already read for SHA-256. The worker now parses those bytes rather than rereading the path, combines source timing with track inventory, and records `has_notes` as a boolean instead of retaining every note message. | Same source bytes, track metadata, source timing, candidate IDs, and track eligibility. |
| Linear canonical-bar materialization | Accepted long streams previously checked every note for every bar when determining emptiness. A difference-array occupancy pass is O(notes + bars), then bulk inserts the identical durable rows. | Note rows and bar boundaries/emptiness use the existing exact interval definition. |
| Direct pinned Brick 3 executable | A real candidate sample measured 0.390 seconds through `uv run` versus 0.369 seconds through the checkout virtualenv command. The configuration selects the latter only when the exact checkout's `.venv/bin/everbar-inspect-midi` exists; otherwise it fails closed. | It invokes the same pinned CLI with the same arguments and working directory. `uv` remains an explicit portable fallback. |

The direct CLI improvement alone is approximately 5% for the isolated invocation
sample. The tree-manifest and canonical-bar improvements avoid repeated work,
so their actual gain depends on Giga's final extracted shape and accepted stream
lengths; measure them from the first completed optimized shard rather than
inventing a percentage.

A direct-versus-`uv run` execution comparison on an upstream MIDI fixture
produced byte-for-byte identical JSON stdout, including the canonical event
hash and decision receipt.

## Patterns checked in other production pipelines

Large dataset systems typically combine deterministic partition ownership,
bounded multiprocessing, batch writes, and durable cache/manifest reuse.
Hugging Face Datasets documents independent `map()` work using `num_proc`,
batched operations, and fingerprinted reusable cache artifacts. Python's
`ProcessPoolExecutor.map()` similarly documents larger chunks as a way to avoid
per-item dispatch overhead for long iterables. Motherlode already follows the
right outer form—durable source partitions and one long-running process per
slot—so it should not introduce a second unbounded per-file process pool.

References:

- [Hugging Face Datasets processing](https://huggingface.co/docs/datasets/process)
- [Hugging Face Datasets cache behavior](https://huggingface.co/docs/datasets/v3.0.2/en/cache)
- [Python `concurrent.futures` chunking](https://docs.python.org/3/library/concurrent.futures.html)

## Rejected ideas

- **GPU/RAPIDS/JAX ingestion:** the actual critical path is stateful MIDI and
  Brick 3, not a tabular or regular numeric workload.
- **Inner multiprocessing per outer shard:** this would compete with the four
  already-saturating Brick 3 children on a four-vCPU host and worsen context
  switching/RAM pressure.
- **Skip/reimplement Brick 3:** forbidden; it is the semantic authority.
- **Drop raw candidates or receipts:** would save I/O only by violating
  provenance/reproducibility requirements.

## Next measured checkpoint

After the optimized code is placed between chunks, compare a completed Giga
partition with the existing PDMX measurements using: candidate streams/second,
Brick-3 process CPU, manifest construction time, source paths/second, and
canonical rows/second. Keep the change only if receipt and canonical-content
equivalence checks pass.
