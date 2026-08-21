# Everbar Motherlode

Everbar Motherlode is a reproducible symbolic-music corpus-construction system for Dreamstream Everbar. It is tooling, not a redistribution of upstream data: raw downloads, converted material, and generated corpus payloads always live outside this Git repository and retain their upstream licenses.

## Scope

**Everbar V1** builds eligible **single pitched musical streams** for direct symbolic MIDI/PerTok use. Polyphony within a stream is allowed. A multitrack source is never collapsed into one V1 example: every eligible pitched track/channel/program candidate is derived independently and passed to Everbar Brick 3.

**Everbar V2** will train synchronized multitrack ensembles. Accordingly, Motherlode retains originals and records source-piece/track identities, sibling tracks, timing/alignment, program/instrument, percussion, source-native role, and track name. Drums are retained for V2 but excluded from V1 pitched eligibility.

Everbar remains the semantic authority. This repository invokes the pinned upstream Brick 3 CLI/API; it does not reimplement or modify corpus policy. See `docs/architecture.md` and `configs/sources/registry.json`.

## Quick start

```bash
uv run everbar-motherlode preflight --root /path/to/motherlode-root
uv run everbar-motherlode build --root /path/to/motherlode-root --config configs/motherlode-v1.toml --resume --detach
uv run everbar-motherlode status --root /path/to/motherlode-root
```

The runner is resumable and only schedules sources whose license lane is eligible. Sources requiring click-through terms, credentials, or unresolved training rights are written to `progress/user-actions.*` and never bypassed.

## Software versus data

`LICENSE` covers only Motherlode code. Dataset terms and required attribution are recorded in `THIRD_PARTY_DATASETS.md`, machine-readable registry records, and build-specific attribution reports. Public availability does not imply training permission.
