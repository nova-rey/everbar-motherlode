# R2 streaming canonical consolidation

`stream-consolidate` is the storage-bounded alternative to `merge-shards` for
a completed distributed run. It is intentionally a new command and does not
change the historical monolithic merge behavior.

## Preconditions

All expected source packages must already be durable and complete under one
immutable run identity:

```
runs/<run-id>/<dataset>/shard-00000-of-<count>/
  shard.sqlite
  shard-receipt.json
  item-ids.json
  manifest.json
  completion.json
```

`completion.json` and `manifest.json` must agree, bind the exact item-ID
ledger, and the downloaded SQLite database must pass `PRAGMA integrity_check`
and contain exactly that ledger. A package that fails any of those checks is
not consolidated.

For the recovered GigaMIDI 96-way run, first package/upload the 16 local
completed worker states one at a time using the existing `stage_shard` /
`publish_shard` contract. Do not construct a monolithic local database.

## Command

```bash
RCLONE_CONFIG=/path/to/protected/rclone.conf \
uv run everbar-motherlode stream-consolidate \
  --workspace /path/with-space-for-one-shard \
  --source-uri direct-s3://evacuate/everbar-motherlode-output \
  --output-uri direct-s3://evacuate/everbar-motherlode-output \
  --run-id azure-gigamidi-96-20260906 \
  --dataset gigamidi \
  --shard-count 96 \
  --consolidation-id gigamidi-96-canonical-v1
```

The command consumes shard indices in deterministic ascending order. It stages
only `shard.sqlite`, not `payload/` MIDI, so its local high-water mark is one
source shard database plus one compact canonical SQLite partition. The
workspace retains only the small canonical-hash index and terminal receipt.

## Output contract and resume

For each shard, the command writes:

```
consolidations/<consolidation-id>/<dataset>/shard-00000-of-<count>/
  canonical.sqlite
  manifest.json
  completion.json
consolidations/<consolidation-id>/canonical-index.sqlite
consolidations/<consolidation-id>/completion.json
```

`canonical.sqlite` contains source piece/track provenance, one materialized
canonical score per Brick-3 event hash, and `stream_provenance` plus
`canonical_dedupe` rows for every accepted source stream. Thus exact duplicate
scores are compacted without discarding source/sibling provenance. The global
canonical-hash index records the deterministic first owner and occurrence
count.

Every artifact is SHA-256 verified after upload. The per-shard completion
marker is written only after the compact partition and current index have been
uploaded successfully. A rerun skips only a package whose output completion
still binds the same immutable source completion manifest. Failed local stages
are retained for diagnosis; successful stages are removed only after all
remote verification has passed.

This path reads stored SQLite receipt data only. It does not reopen raw MIDI,
invoke Brick 3, alter corpus policy, mutate distributed packages, or delete
any source package. It is not yet a training snapshot builder; a later,
separate view builder must consume the compact partitions and their dedupe
ledger.
