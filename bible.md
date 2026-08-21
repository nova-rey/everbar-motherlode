# Everbar Motherlode build bible

Append-only engineering record.

## 2026-08-21 — Brick 3 receipt identity repair

Corrected Motherlode’s canonical hash extraction to use Everbar Brick 3’s authoritative `canonical.event_sha256` field. Added a resumable `reconcile` command that backfills existing immutable receipts, and ensured `--resume` skips completed datasets instead of reprocessing them.

## 2026-08-21 — Official PDMX automated acquisition

Added safe tar extraction with traversal and link rejection, and replaced PDMX's manual placeholder with its official Zenodo MIDI archive endpoint. The registry records the required `no_license_conflict` subset policy; no rights are inferred from archive availability.

## 2026-08-21 — Enforce PDMX license-conflict subset

PDMX derivation now downloads and intersects the official `no_license_conflict` manifest with archive MIDI paths before any V1 candidate is created. This makes the registry’s public-domain eligibility restriction executable rather than documentary.

## 2026-08-21 — Correct next-wave license queue

Corrected GigaMIDI v2 to its official gated CC-BY-NC-4.0 record and Aria-MIDI to its official gated CC-BY-NC-SA-4.0 record. Aria’s source-code repository is no longer misrepresented as a downloaded corpus. Existing false completion state is reconciled into an explicit user-action gate.
