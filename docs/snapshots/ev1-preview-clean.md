# EV1 preview clean snapshot

`everbar-motherlode snapshot-preview` creates an immutable, external V1
training snapshot from only completed PDMX shard receipts and the completed
POP909 receipt database.  It is deliberately read-only with respect to the
live Motherlode root: incomplete shards are not members, and the live queue is
never paused or rewritten.

The builder performs exact raw/canonical provenance preservation and collapses
only equal Brick-3 canonical event hashes.  It writes canonical notes/bars,
source-family-safe 80/10/10 splits, PerTok IDs, Brick 4 cap evidence, a
preview-only block format and active-length profile, and the exact packed-view
layout consumed by the existing Brick 8 loader.  It never reads source MIDI or
invokes Brick 3 after membership is frozen.

The snapshot is `EV1_PREVIEW_ONLY`: a V1 single pitched stream view without
conditioning.  Its cap and active-length profile are not global Brick 4/6
production authority.  The canonical SQLite database remains alongside the
packed data so later feature extraction needs neither raw acquisition nor
PerTok decoding.
