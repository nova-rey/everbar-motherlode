# Canonical corpus versus derived features

Motherlode's expensive work happens once: preserve the raw source, derive a
V1 pitched candidate without merging its ensemble, apply the audited
conversion boundary, and obtain an ACCEPT receipt from the pinned Everbar
Brick 3 authority. An accepted Brick 3 receipt contains the canonical score
payload, not only its hash. Motherlode materializes that payload into additive
SQLite canonical tables:

- `canonical_streams`: accepted stream identity, Brick 3/policy binding,
  canonical timing basis, and source-qualified provenance;
- `canonical_notes`: ordered canonical onset, duration, end tick, pitch,
  velocity, onset-bar and end-bar membership;
- `canonical_bars`: meter-relative boundaries, including empty bars inside the
  canonical represented span; and
- `source_pieces` / `source_tracks`: V2 source-family inventory, including
  non-V1 drum tracks, program/channel identity, track names, timing, and any
  source-native role label.

The canonical tables are a post-Brick-3 analysis view. Their note rows are
copied from `dreamstream-everbar.canonical-score/v1` in the stored acceptance
receipt, so they are not a fresh interpretation of raw MIDI. The original raw
bytes and pre-Brick-3 derivative remain upstream in the authority chain.

`canonical_span_end_tick` is the final sounding tick in the canonical score.
It gives exact bar membership, cross-bar duration, overlap, occupancy, and
internal empty-bar semantics. Source-only trailing end-of-track silence remains
recorded separately in `source_timing_json`; it is not silently promoted into
canonical musical content.

## Backfill and independent feature passes

`everbar-motherlode backfill-canonical --root ROOT` is idempotent. It reads
only durable SQLite item details and stored Brick 3 receipts, including shard
state databases. It does not download, parse raw MIDI, invoke Brick 3, or
decode PerTok. This makes it safe to run after an interrupted build once a
database is quiescent or on a completed shard package.

Features are replaceable, versioned outputs, never inputs to canonical hashes.
For example:

```bash
everbar-motherlode extract-features --root ROOT --extractor-id primitive-v1
```

creates `features/primitive-v1/features.sqlite` and a manifest. The proof
extractor reads only `canonical_notes` and `canonical_bars` and calculates a
small per-bar set: note/onset counts and rates, mean/max polyphony, occupancy
and rest fraction, median duration/pitch, pitch range, and mean velocity.
Later extractors (`articulation-v1`, `repetition-v1`, or a V2 cross-track
extractor) use another extractor ID/database. Replacing them never changes
Brick 3 receipts, canonical stream IDs, dedupe identities, or provenance.

For V2, join `canonical_streams.source_piece_id` to `source_tracks` and the
stored sibling IDs. V1 uses one stream; it does not erase its original ensemble
or percussion authority.
