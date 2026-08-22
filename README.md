# Everbar Motherlode

Everbar Motherlode is a reproducible symbolic-music corpus-construction system for Dreamstream Everbar. It is tooling, not a redistribution of upstream data: raw downloads, converted material, and generated corpus payloads always live outside this Git repository and retain their upstream licenses.

## Scope

**Everbar V1** builds eligible **single pitched musical streams** for direct symbolic MIDI/PerTok use. Polyphony within a stream is allowed. A multitrack source is never collapsed into one V1 example: every eligible pitched track/channel/program candidate is derived independently and passed to Everbar Brick 3.

**Everbar V2** will train synchronized multitrack ensembles. Accordingly, Motherlode retains originals and records source-piece/track identities, sibling tracks, timing/alignment, program/instrument, percussion, source-native role, and track name. Drums are retained for V2 but excluded from V1 pitched eligibility.

Everbar remains the semantic authority. This repository invokes the pinned upstream Brick 3 CLI/API; it does not reimplement or modify corpus policy. See `docs/architecture.md` and `configs/sources/registry.json`.

## Canonical corpus and future features

The expensive ingestion pass is not where conditioning measurements belong.
Every Brick-3-accepted stream persists an analysis-ready canonical event/note
base plus V2 source-family provenance. Future deterministic feature passes read
that durable post-Brick-3 view and write versioned derived tables; they do not
need raw MIDI, PerTok decoding, reacquisition, or another Brick 3 run. See
[canonical feature base](docs/canonical-feature-base.md).

## Quick start

```bash
uv run everbar-motherlode preflight --root /path/to/motherlode-root
uv run everbar-motherlode build --root /path/to/motherlode-root --config configs/motherlode-v1.toml --resume --detach
uv run everbar-motherlode status --root /path/to/motherlode-root
```

The runner is resumable and only schedules sources whose license lane is eligible. Sources requiring click-through terms, credentials, or unresolved training rights are written to `progress/user-actions.*` and never bypassed.

## Disposable-worker / GitHub Actions shards

The normal deterministic worker interface is `shard --dataset ID --partition-index I --partitions N`. Ownership is SHA-256 of the dataset ID and source-relative path, so `N` shards have no overlap and do not depend on scheduling order or randomness. `distributed-shard` wraps that same command for disposable machines: it fetches immutable input from a caller-selected `file://` or `rclone` URI, stages a run-scoped package, and publishes `completion.json` last.

See [distributed GitHub Actions operations](docs/github-actions-distributed-prep.md). The repository never stores raw or generated corpus payloads; Actions secrets provide storage credentials only during manually dispatched, protected write jobs.

The protected **Stage authorized corpus input** workflow acquires one
registry-approved raw source into private R2 before its processing run. Its
raw-file hash manifest and `staging-completion.json` marker are written only
after the payload upload succeeds. This keeps the disposable processing swarm
independent from this workstation and makes a failed staging attempt retryable.

## Software versus data

`LICENSE` covers only Motherlode code. Dataset terms and required attribution are recorded in `THIRD_PARTY_DATASETS.md`, machine-readable registry records, and build-specific attribution reports. Public availability does not imply training permission.
