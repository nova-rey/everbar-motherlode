# Remaining-source local autopilot

`remaining-sources-autopilot` is the durable local-only controller for the
rest of the automated, training-allowed Motherlode raw sources. It is not a
replacement for corpus semantics: existing `shard`, Brick 3, distributed
publication, verification, and receipt-only streaming consolidation remain
the authorities.

## Eligibility boundary

The controller inventories every registry entry and writes two durable
receipts under `progress/`:

- `remaining-sources-inventory.json` classifies each entry as queued,
  completed-and-skipped, or gated;
- `remaining-sources-gates.json` explains every non-automated entry.

It queues only `training=ALLOWED`, `role=raw`, non-`manual_gated` sources. The
plan explicitly names already-completed sources, so it cannot re-run their
raw data. Manual legal/terms gates remain gates until separately authorized.

## Per-source protocol

Sources advance sequentially to bound local storage. For one source the
controller: checks a configured compressed-input/extraction/derivation
headroom envelope; downloads and extracts once; launches deterministic
partitions (up to the plan worker limit); packages and verifies every shard in
R2; receipt-reconciles and streams compact canonical partitions; then removes
only verified local raw/extracted/derived payload and shard state. Conversion
receipts remain local audit evidence.

The current plan intentionally uses 12 partition workers and a 20 GiB
unallocated reserve. It writes live state to
`progress/remaining-sources-autopilot.json`; restarting the exact command
skips durable source completion receipts and completed partition receipts.

```bash
RCLONE_CONFIG=/home/rey/.config/rclone/rclone.conf \
  /home/rey/everbar-motherlode/.venv/bin/python -m everbar_motherlode.cli \
  remaining-sources-autopilot \
  --root /home/rey/motherlode-root \
  --config /home/rey/everbar-motherlode/configs/motherlode-v1.toml \
  --plan /home/rey/everbar-motherlode/configs/remaining-sources-autopilot.json
```

R2 completion markers and compact canonical receipts are the durability
boundary; a local source tree is never deleted before both paths complete.
