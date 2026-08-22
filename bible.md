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

## 2026-08-21 — Concurrent acquisition prefetch

Added an independent, bounded prefetch runner for all automated, training-eligible raw sources and their source-qualified metadata. It never mutates CPU-pipeline dataset state or exposes partial files as complete; downloads use existing resumable `.part` behavior and atomically promote only completed artifacts. This permits I/O acquisition to overlap with PDMX/Brick 3 processing.

## 2026-08-21 — Authenticated Hugging Face acquisition

Added a credential boundary for Hugging Face payload URLs: Motherlode reads a local CLI token only from established environment/cache locations, supplies it as an HTTP authorization header, and never records the token in repository files, logs, receipts, or reports. This lets an operator-approved headless `hf auth login` authorize gated downloads without broadening license lanes.

## 2026-08-21 — Live uncommitted-batch progress and ETA

Added a detached monitor that derives live stream count from filesystem artifacts while a large transaction remains uncommitted. For PDMX it reads the official eligible-path manifest, publishes percent complete, a measured PDMX-only stream rate, and a bounded stage ETA. It deliberately does not fabricate an overall corpus ETA until downstream source inventories exist.

## 2026-08-21 — Isolated parallel source workers

Added source-shard workers that share immutable/downloaded inputs and derived artifact paths, but use independent SQLite state databases so a long PDMX transaction cannot block other CPU cores. Completed shard receipts are merged deterministically into central state only after the central writer is idle, then marked DONE to avoid duplicate processing.

## 2026-08-21 — Remote Everbar checkout override

Fixed the deployment boundary so a detached build can select its pinned Everbar checkout through `EVERBAR_CHECKOUT`, rather than inheriting a workstation-only absolute path. Existing Lightning Brick 3 failures caused by the missing local path are classified as invalid execution receipts and must be regenerated; no corpus-policy behavior is changed.

## 2026-08-21 — Performance-flattening-v1 conversion boundary

Accepted `performance-flattening-v1` before Brick 3: original source and candidate MIDI remain immutable; CC64 sustain and CC66 sostenuto are rendered into note lifetimes; CC67 is explicitly discarded as unrepresentable for V1; and verified zero-duration note pairs are dropped. Every conversion writes source/output hashes, operation counts, and a receipt before the converted stream reaches Brick 3.

Bounded remote validation confirmed a formerly controller-rejected MAESTRO sample is accepted after conversion. The converted ASAP sample no longer has the zero-duration rejection but surfaces distinct CC121 and unsupported-meter policy evidence, which remains unmodified and queued for a separate decision.

## 2026-08-21 — CC121 reset consumption

Extended `performance-flattening-v1` to consume channel-local CC121 Reset All Controllers at the exact source tick: sustain/sostenuto state and latches reset, pedal-deferred notes release at that tick, and the controller event is removed only after that state transition. Conversion receipts now count consumed CC121 resets; unsupported meters remain Brick 3 evidence only.

A deterministic 64-stream-per-dataset remote audit yielded 64/64 MAESTRO accepts and 46/64 ASAP accepts. CC121 eliminated remaining semantic-control rejections in both samples. Unsupported meter affected 8/64 ASAP and 0/64 MAESTRO samples; no meter or PerTok change was made.

## 2026-08-22 — Deterministic source partitioning

Added hash-stable source-path partitioning for independent shard workers. It permits PDMX to use multiple non-overlapping CPU workers while preserving source IDs, candidate IDs, conversion policy, Brick 3 authority, and deterministic merge behavior.

## 2026-08-22 — GigaMIDI split-archive preparation

Added a source-specific, lock-serialized safe extraction step for GigaMIDI's documented outer and nested split ZIPs. Each nested archive receives an atomic expansion marker only after completion, so interrupted preparation resumes and partition workers never treat the outer ZIP bytes as corpus MIDI files.

## 2026-08-22 — Disposable distributed shard boundary

Added a run-scoped shard package around the existing hash-stable corpus partition primitive. A completed shard exports only its own SQLite state, derived candidate/converted MIDI, and conversion receipts, then publishes an immutable completion marker last. This supports independent disposable workers and retrying only failed shards without changing corpus-policy semantics.

The GitHub Actions smoke path now selects POP909 as its intentionally small first source, while the same deterministic interface remains available for PDMX and GigaMIDI only after storage and runner limits are measured.

## 2026-08-21 — Brick 3 semantic-review handoff

Added a development-thread brief separating the invalid remote-path receipts from genuine MAESTRO pedal-controller and ASAP sustain/zero-duration-note findings. The brief records sample evidence and decision options while preserving the rule that Motherlode must not alter Brick 3 policy or silently repair source semantics.
