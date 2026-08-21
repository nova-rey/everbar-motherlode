# Everbar Motherlode build bible

Append-only engineering record.

## 2026-08-21 — Brick 3 receipt identity repair

Corrected Motherlode’s canonical hash extraction to use Everbar Brick 3’s authoritative `canonical.event_sha256` field. Added a resumable `reconcile` command that backfills existing immutable receipts, and ensured `--resume` skips completed datasets instead of reprocessing them.

## 2026-08-21 — Official PDMX automated acquisition

Added safe tar extraction with traversal and link rejection, and replaced PDMX's manual placeholder with its official Zenodo MIDI archive endpoint. The registry records the required `no_license_conflict` subset policy; no rights are inferred from archive availability.
