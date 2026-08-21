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

## 2026-08-21 — User-accepted research acquisition wave

Recorded the user’s explicit acceptance of supplied official terms/download URLs for GigaMIDI, Aria-MIDI, ComMU, MAESTRO, ASAP, EMOPIA, and Groove MIDI. These sources are enabled only in the `RESEARCH_MAX` lane; their NC/SA conditions remain preserved in provenance and do not become permissive/commercial rights.

The provided GuitarSet Zenodo annotation archive is recorded as a source-qualified provenance reference, not misrepresented as the original symbolic payload. Reconciliation now makes Aria eligible for retry when the current registry has authorized automated acquisition, and downloaded source metadata remains separate from payload identity.

## 2026-08-21 — Source-level attribution for accepted endpoints

Added a durable attribution table for each user-supplied acquisition endpoint, including source URLs, canonical citations, and Motherlode’s precise conversion boundary. Build attribution remains inclusion-based: it names only datasets that the run actually completes, while the static record documents authorization and provenance without claiming ownership or broader rights.

## 2026-08-21 — Remote launch identity receipt

Detached launch receipts can now receive the exact staged tooling revision through `MOTHERLODE_REPO_SHA`. This preserves an honest build identity when a cloud worker runs a transferred, pinned tooling bundle rather than a checkout with remote Git credentials.
