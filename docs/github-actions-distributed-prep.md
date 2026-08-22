# GitHub Actions distributed preparation

This workflow is an orchestration layer around the normal Motherlode command; it does not change conversion, Brick 3, source IDs, filtering, or license lanes.

## Existing pipeline and shard contract

The preparer is `everbar-motherlode shard --dataset DATASET --partition-index I --partitions N`. It reads the registry-selected raw source artifact, extracts it safely, derives every eligible V1 pitched stream, applies `performance-flattening-v1`, and invokes the pinned Everbar Brick 3 CLI. Its stable owner is `SHA-256(dataset_id + NUL + source-relative-MIDI-path) % N`. There is no random sampling or work-stealing. A source path has exactly one owner for a given `N`; output IDs are semantic hashes, not process IDs or timestamps.

The normal local root contains immutable raw data, extracted inputs, derived MIDI/pre-Brick-3 MIDI, conversion receipts, a per-shard SQLite state database, and a completion receipt. A central `merge-shards` can aggregate completed SQLite shards after all workers finish. Ordering in a merged SQLite table can differ, but item IDs, candidate bytes, receipts, and Brick 3 decisions are invariant.

## Disposable runner interface

Use the wrapper below on a fresh worker:

```bash
everbar-motherlode distributed-shard \
  --root "$RUNNER_TEMP/motherlode" \
  --dataset pdmx --shard-index 7 --shard-count 20 \
  --run-id 20260822-pdmx-smoke \
  --input-uri r2:motherlode-input \
  --output-uri r2:motherlode-output
```

`input_uri` contains the already-authorized immutable source layout `raw/<dataset-id>/`. The worker never relies on a sibling runner. It emits this immutable package:

```text
runs/<run-id>/<dataset>/shard-00007-of-00020/
  shard.sqlite
  shard-receipt.json
  payload/derived/...                 # this shard's candidates and converted MIDI
  payload/receipts/conversion/...     # corresponding conversion receipts
  manifest.json
  item-ids.json                     # exact IDs for final coverage/overlap checks
  completion.json                     # uploaded last; success marker
```

For `file://` targets, publication is a same-filesystem atomic rename. For an rclone object-store target, payload is uploaded first and `completion.json` is uploaded last. A completed destination is rejected unless `--force` is explicit. Retry the one failed shard with the identical run ID/index/count; already completed peers are neither read nor recomputed.

## Storage and credentials

The repository chooses no paid vendor. It supports any rclone-compatible storage (S3, Cloudflare R2, Backblaze B2, or a self-hosted S3-compatible endpoint) and `file://` for local tests/mounted storage. Storage cost, retention, and data-license obligations are the operator's responsibility.

Create a protected GitHub Environment named `corpus-write`, and add `MOTHERLODE_RCLONE_CONFIG_B64`: base64 of a least-privilege rclone configuration allowed to read the selected input prefix and write only the selected output prefix. It is used only in the manual `workflow_dispatch` workflow. Do not enable it for pull requests or forks, and do not put credentials, token strings, or generated corpus data in Git.

## Launch sequence

1. Run the local equivalence test (`uv run pytest -q`). It proves stable complete, non-overlapping partition ownership and package/retry behavior on a deterministic fixture.
2. Dispatch **Distributed corpus preparation** with `shard_count=2`, a small source/subset stored at the input URI, and a new run ID. Inspect both `completion.json` packages.
3. Run 4 or 8 shards on a representative subset. Compare `item_ids_sha256` coverage after aggregating item IDs, inspect elapsed time/output bytes in each manifest, and check that no completion marker is missing.
4. Only then dispatch 20 shards. Do not use Actions artifacts as corpus storage.

## Capacity and risks

GitHub-hosted standard Linux runners are disposable and normally expose about four vCPUs, but the current Brick 3 invocation is a sequential subprocess boundary inside one Motherlode shard. Start with one worker per runner; the workflow records timing, item count, output bytes, and items/sec. Benchmark 1, 2, and 4 local subshards per runner on a representative subset before increasing intra-runner concurrency—four interpreter processes may compete for memory, startup, and object-store bandwidth.

The largest risks are source download/object-storage bandwidth, source-specific archive expansion, GitHub job duration limits, and uneven MIDI complexity. Hash ownership is exact but not cost-aware; compare per-shard elapsed times in the 4/8-shard test before a 20-way run. If imbalance is material, create and version a size-aware manifest; do not replace deterministic ownership with runtime work stealing.

The workflow is deliberately manual-only and has not launched a paid or full 20-way corpus run. A final aggregation/verifier should read only `completion.json` packages, require every expected shard index exactly once, union item IDs, reject overlaps, and then invoke the existing `merge-shards` logic in a dedicated trusted environment.

The workflow's `verify` job performs those package-level checks and writes `runs/<run-id>/<dataset>/run-manifest.json`. It fails if a shard is missing, a marker is malformed, or any item ID appears in two shard packages. The same check is available outside Actions:

```bash
everbar-motherlode verify-distributed-run --root /tmp/verify \
  --dataset pdmx --shard-count 20 --run-id 20260822-pdmx-full \
  --output-uri r2:motherlode-output
```
