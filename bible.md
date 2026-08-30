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

## 2026-08-22 — Cloudflare R2 persistent storage handoff

Provisioned private `everbar-motherlode-input` and `everbar-motherlode-output` R2 buckets and a protected `corpus-write` GitHub Environment secret containing the rclone configuration. The credential was verified with an S3-compatible bucket listing and is never represented in repository files, logs, or workflow output. Distributed-preparation documentation now uses the provisioned rclone URIs.

## 2026-08-22 — Distributed smoke hardening

Corrected the partition-state label so the deterministic worker and distributed publisher use the same fixed-width durable path. The workflow now uses `pipefail`, checks out the exact private Everbar authority through a protected read-only deploy key, and verifies the upstream SHA before any Brick 3 invocation. A real smoke run can no longer report a worker failure as a successful preparation job.

## 2026-08-22 — Immutable Everbar authority bundle

The configured Brick 3 SHA is present in the trusted local Everbar checkout but not fetchable from the private remote. The public worker therefore restores a checksummed private R2 Git bundle, checks out that exact SHA, and fails closed on either hash mismatch. This preserves the pinned semantic authority without silently changing policy or publishing the upstream repository.

## 2026-08-22 — Authority bundle checksum receipt

The immutable bundle sidecar stores the digest alone. The worker compares that digest directly to the fetched bundle's SHA-256 rather than treating it as a filename-bearing `sha256sum -c` record. The verification remains fail-closed while matching the storage receipt format.

## 2026-08-22 — Canonical feature-base persistence

Added additive canonical stream, note, bar, source-piece, and source-track tables populated directly from Brick 3 ACCEPT receipts. Accepted item updates now retain their earlier source-family provenance rather than overwriting it. The canonical backfill and `primitive-v1` proof extractor use only persisted SQLite/receipt data, making later feature versions independent from raw MIDI, PerTok decoding, Brick 3, acquisition, and dedupe.

## 2026-08-22 — Immutable Actions input staging

Added a protected manual staging workflow for each registry-approved raw source. It acquires the official payload directly to a disposable runner, emits a source-qualified SHA-256 manifest, uploads private R2 data first, and publishes the staging completion marker last. Processing workers can therefore consume only verified staged inputs and retryable staging remains independent of CPU shard execution.

## 2026-08-22 — Approved-source Actions dispatch parity

Expanded the distributed processing workflow selector to cover every current approved raw source, including ComMU, Groove MIDI, and EMOPIA. The selector remains a registry-aligned launch guard; it does not make unclear, gated, overlay, superseded, or synthetic sources training eligible.

## 2026-08-22 — Runner package-index isolation

Rclone installation now refreshes only the official Ubuntu package source on disposable Actions runners. A transient inconsistent third-party Chrome package index can no longer abort an otherwise healthy corpus shard before it contacts R2; failed shards remain safely retryable under their existing immutable run IDs.

## 2026-08-22 — Stage-to-swarm automation

The protected input-staging workflow now dispatches its own immutable processing swarm after and only after R2 staging completes. The shard count remains an explicit bounded input, the run ID is tied to the staging run, and a staging failure cannot start workers against incomplete input. This removes manual handoff between successful acquisition and CPU processing without granting write credentials to untrusted events.

## 2026-08-22 — Authorized Lightning GigaMIDI transfer

GigaMIDI staging has a protected Lightning-to-R2 path for the already acquired archive and its metadata companion. It transfers through disposable GitHub Actions storage using a protected SSH deploy key, hashes both source-qualified files, publishes payload before the completion marker, and immediately dispatches the normal deterministic shard swarm. This deliberately avoids another Hugging Face request and keeps the credential boundary limited to the protected environment.

## 2026-08-22 — Source-specific license evidence review

Reviewed the manual and unclear acquisition set against official project, dataset-card, and paper evidence. ATEPP, GiantMIDI-Piano, and Los Angeles MIDI now have explicit source-qualified records; only Los Angeles has an automatable public payload endpoint. PiJAMA, MID-FiLD, Pop1K7, and Symphony MIDI remain excluded because a code license, paper, or project page does not establish payload training rights. The review preserves user term acceptance without treating it as a substitute for upstream rights.

## 2026-08-22 — Deterministic PDMX partition manifest

PDMX workers now build one lock-protected immutable mapping from the official no-license-conflict list to deterministic partition ownership. Later shards consume only their assigned path list instead of repeatedly walking the full extracted tree and rebuilding the allow-list. Raw hashes and track note inventory are reused within each source piece; neither change alters candidate identities, source ownership, Brick 3 authority, or V2 provenance.

## 2026-08-22 — Correct resumable PDMX chunk worker

Added the durable worker launcher used by the Lightning deployment. It checks a quoted receipt state, uses the same fixed-width partition labels as Motherlode, skips only validated completed chunks, and assigns four deterministic slots. This prevents resumed runs from silently recomputing completed PDMX chunks.

## 2026-08-22 — Four-slot resumable wave controller

Generalized the validated receipt guard for PDMX and GigaMIDI and added a four-slot Lightning wave controller. The controller serializes GigaMIDI's one-time nested extraction, then resumes independent deterministic partitions and performs only the existing reconciliation/merge handoff. It does not change corpus semantics or introduce shared writable worker state.

## 2026-08-22 — Atomic Lightning wave deployment

Added a deployment handoff that stops only scheduler parents, waits for their in-flight chunks to finish, retains the prior source tree as rollback evidence, then starts the validated four-slot workers and next-wave controller. This lets performance changes enter between chunks without killing, repeating, or silently changing completed corpus work.

## 2026-08-22 — Core-aware fourth-slot handoff

The fourth PDMX slot is represented by a durable waiting wrapper while the existing MAESTRO/Aria job owns the fourth CPU. It execs the deterministic worker only after that job exits, so the controller retains four-slot completion semantics without avoidable CPU oversubscription.

## 2026-08-22 — GPU feasibility review rejects ingestion rewrite

The active Motherlode path is variable-length MIDI parsing, stateful performance flattening, pinned Brick 3 subprocess authority, and durable receipt I/O—not DataFrames, text tokenization, or a regular numeric kernel. Measurements on the CPU-only production worker show Brick 3 children consuming the available cores, so no CUDA, RAPIDS, or JAX rewrite is justified. A later independent canonical-event fingerprint or feature batch remains a contained candidate for a GPU benchmark without changing corpus semantics.

## 2026-08-22 — Production-path performance pass

Motherlode now avoids a repeated whole-tree GigaMIDI scan for every worker by publishing one deterministic lock-protected partition manifest. It also fuses source timing/inventory into the required raw-byte parse, eliminates retained note-message lists used only as booleans, makes canonical bar occupancy linear in notes plus bars, and permits a fail-closed direct invocation of the exact pinned Everbar checkout CLI. These are strictly operational improvements: raw bytes, source identities, partition ownership, receipts, Brick 3 authority, and canonical output remain unchanged.

## 2026-08-22 — Detached watchdog escalation

Added a read-only hourly Motherlode watchdog and user-level systemd timer. It verifies worker/controller/monitor liveness, receipt freshness, and conversion movement through the persistent Lightning root, then uses Codex's thread queue to request repair only on a debounced fault. The watchdog never restarts workers or makes corpus-policy decisions itself.

The deployed user service pins the local Codex CLI directory in its execution PATH. This preserves the queue-based escalation mechanism under systemd's intentionally minimal environment.

The remote probe passes the configured corpus root explicitly into the SSH command, so receipt and PID checks execute against the intended persistent root rather than inheriting an absent remote shell variable.

The detached health probe retries transient SSH failures three times before it
escalates. One Lightning routing hiccup therefore cannot wake an expensive
repair turn while healthy workers continue processing.

Receipt age is now observational while the deterministic worker/controller set
is alive: chunk-boundary publication can legitimately be older than fifteen
minutes. The separate durable conversion-movement grace remains the stall
alarm, avoiding false escalation during long valid Brick 3 chunks.

The movement grace period now resets when Lightning resumes the scheduler with
new PIDs. This distinguishes a safe receipt-preserving studio restart from a
stalled active wave.

The watchdog also treats the flat per-dataset `prebrick3` directory mtime as a
cheap durable movement witness. Long PDMX/Giga chunks create derivative files
well before their aggregate receipt is published; this prevents a healthy,
actively converting chunk from being mislabeled `no_conversion_progress`.

## 2026-08-22 — Immutable EV1 preview snapshot seam

Added a read-only snapshot builder for completed clean PDMX partitions plus
completed POP909 receipts. It freezes exact canonical membership, preserves
all collapsed provenance edges, uses source-family-safe splits, materializes
canonical notes/bars without raw MIDI or Brick 3, and writes the existing
Brick 8 packed-view contract. The live Motherlode queue is not a dependency or
write target of this operation.

The CPU-only constructor emits the already-specified Brick 6 profile JSON
directly, avoiding a Torch import that belongs only to training/sampling. This
keeps receipt-to-package construction usable on the dedicated corpus host.

## 2026-08-22 — POP909 fast-lane proof-of-concept builder

Added an isolated POP909-only preparation path that reuses the verified
official source archive, indexes all 909 source pieces and sibling tracks,
derives MELODY/BRIDGE/PIANO candidates, invokes the pinned Everbar Brick 3
authority in-process, preserves canonical feature-base receipts, performs
exact within-POP909 dedupe and source-piece splits, profiles Brick 4 caps, and
materializes a clearly non-production packed training view. The path records
source/license/revision evidence and has no access to Motherlode's central
production state.

## 2026-08-27 — Deterministic Lightning watchdog SSH identity

The Motherlode watchdog now explicitly uses the configured Lightning private
key with bounded connection attempts and keepalives. Systemd user services do
not reliably inherit an interactive ssh-agent; this change prevents a healthy
studio from being reported as unavailable solely because `SSH_AUTH_SOCK` is
absent. It does not modify runner or corpus semantics.
## 2026-08-27 — Reliable PDMX worker heartbeat

Added a receipt-safe worker liveness monitor. It only writes an atomic
heartbeat from durable worker PID receipts; it neither claims shard completion
nor changes shard assignment. The PDMX workers remain responsible for skipping
only `COMPLETE` receipts when resumed.

## 2026-08-28 — Watchdog supervisor receipt-name compatibility

The Motherlode watchdog now recognizes the receipt-safe current supervisor PID
names (`queue-gigamidi-after-pdmx.pid` and `monitor-pdmx-workers.pid`) while
retaining the historical aliases for older live deployments. This fixes false
`wave_controller_dead` and `progress_monitor_dead` alerts without changing
corpus work, shard ownership, or completion semantics.
## 2026-08-29 — Receipt-safe PDMX recovery watchdog compatibility

The Motherlode watchdog now recognizes the `pdmx-resume-worker-v2-*` and
`pdmx-resume-monitor-v2` runtime names used after a Lightning studio restart.
An absent next-wave controller is no longer considered a failure while a live
receipt-safe PDMX recovery wave is actively processing durable, incomplete
partitions. Completion receipts remain the sole shard-resume authority.

## 2026-08-29 — V2 live projection and candidate features

Added read-only V2 projection and candidate-feature modules over canonical
SQLite. Sustain-aware half-open bar occupancy, maximal live segments, original
source-compositional lifecycle positions, four-span eligibility, and fifteen
versioned candidate controls are covered by synthetic tests. Canonical tables
and source artifacts remain unchanged; characterization remains provisional
until an authoritative preview snapshot is available.

Feature extraction now computes polyphonic-time fraction from interval sweep
state rather than a stream-wide maximum, and the CLI can attach the persisted
projection rows before writing feature views.

Updated the canonical-feature documentation with the V2 projection and
candidate-control boundaries. No production snapshot, conditioning schema, or
canonical-table mutation was introduced.

The extractor was optimized to sweep each stream's notes once across ordered
bars and supports bounded stream selection for CPU probes; a 100-stream probe
completed in 0.73 seconds and produced deterministic feature and
characterization hashes. This remains non-authoritative because the approved
30k preview snapshot is not present on this host.

The V2 CLI now exposes explicit read-only `project-v2` and `characterize-v2`
commands for these derived views; they require a caller-supplied canonical
SQLite path and never infer or download corpus authority.

## 2026-08-29 — V2 sidecar artifact bindings

Added deterministic content-addressed manifests, dependency-free int64 index
writers, and a query-only canonical SQLite opener for derived V2 projection
and feature artifacts. Projection and feature views now bind their files and
canonical identity without mutating Motherlode authority; synthetic tests
cover read-only enforcement and reproducibility. No preview corpus was built.

The authority-gated `build-v2-sidecar` command now validates the exact frozen
V1 manifest and mmap array shapes before creating projection and feature
sidecars. It fails before output creation when the base snapshot is missing or
wrong; no snapshot reconstruction path was added.

## 2026-08-30 — V2 projection stream-scoped window matching

V2 eligible-window matching now keys projected bars by `(stream_id,
bar_index)` and rejects ambiguous streamless matches when multiple streams
share bar indices. This preserves the synthetic single-stream interface while
preventing cross-stream fallback from contaminating source-family or segment
identity. Focused V2 projection, artifact, and sidecar tests pass.
