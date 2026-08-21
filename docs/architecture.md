# Architecture

State is durable SQLite (transactional item queue and stage transitions) plus append-friendly JSONL/Parquet-ready exports; it is deliberately not a giant in-memory JSON object. SQLite is suitable for millions of indexed rows and resumable claims. Analytical reports are produced by streaming aggregation and can be exported to Parquet when DuckDB is installed.

Stable IDs are SHA-256 namespaces over semantic inputs: dataset/version, artifact bytes, source-piece path within artifact, source-track structure, derived stream, canonical score, and family fingerprint. Paths, wall-clock time, and machine names are excluded from semantic IDs.

The state machine is `DISCOVERED → LICENSE_VERIFIED → DOWNLOAD_PENDING → DOWNLOADING → DOWNLOADED → HASH_VERIFIED → EXTRACTED → INDEXED → DERIVED → BRICK3_COMPLETE → FINGERPRINTED → DEDUPE_COMPLETE → OVERLAY_COMPLETE → PROFILE_COMPLETE → DONE`, with durable failure terminal states. Raw downloads are immutable after verification.

Download adapters expose `discover`, `license`, `estimate_size`, `download`, `verify`, `extract`, and `index`. HTTP/GitHub archive are currently implemented; `gated`, `manual`, and unsupported formats create user actions. HTTP uses `.part`, Range resume, bounded exponential backoff, SHA-256 verification, and atomic rename. Archive extraction rejects absolute and traversal members.

Deduplication preserves provenance edges and rights: raw SHA, Brick-3 canonical identity, metadata-insensitive identity, tempo and transposition normalized fingerprints, then MinHash-style shingle buckets for scalable candidate generation. Only provable raw/canonical duplicates are collapse candidates; variants cluster but remain represented.

Resource guard reserves configured free space. On guard breach no new work starts and `RESOURCE_PAUSED` is written; verified data is never deleted automatically.
