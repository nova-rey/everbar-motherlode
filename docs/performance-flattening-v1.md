# performance-flattening-v1

`performance-flattening-v1` is Motherlode's accepted deterministic conversion
boundary immediately before Everbar Brick 3. It exists to create a V1
single-stream note representation while retaining the exact original source
and unflattened derived candidate.

## Rules

- Preserve the raw source artifact and the initial derived V1 MIDI unchanged.
- Render CC64 sustain into delayed note-off lifetimes.
- Render CC66 sostenuto into delayed note-off lifetimes for notes sounding when
  the pedal is engaged.
- Consume CC121 Reset All Controllers at its exact source tick: clear the
  channel's sustain/sostenuto state and latches, release any pedal-deferred
  note-offs at that tick, then remove the reset event from the V1 derivative.
- Discard CC67 soft-pedal events because V1 has no representable equivalent.
- Drop a note-on/note-off pair only when it is verified to have zero duration.
- Preserve all other events and leave every remaining Brick 3 decision to the
  pinned Everbar authority.

## Receipt

For every candidate, Motherlode writes
`receipts/conversion/<candidate-id>.json`, containing:

- policy ID;
- source candidate and output paths;
- SHA-256 hashes of the unflattened and flattened MIDI bytes;
- counts for CC64/CC66 render operations, CC67 removals, CC121 resets,
  zero-duration drops, and any end-of-track note-off flushes; and
- a receipt hash.

The flattened MIDI is written beneath `derived/<dataset>/prebrick3/`. It is a
derived conversion, never an original source file.

## Bounded validation evidence

The original MAESTRO sample rejected for CC64/CC66/CC67 was accepted after
conversion with no controller or invalid-note reason codes. The original ASAP
sample no longer produced `REJECT_INVALID_NOTE` after its zero-duration no-op
was removed. CC121 is now consumed by this policy; unsupported meter and other
non-covered event classes remain Brick 3 evidence.

After CC121 consumption was added, a deterministic 64-stream-per-dataset audit
against the pinned Lightning Everbar authority reported:

| Dataset | Accepted | Rejected | Accept rate | Unsupported-meter rate | Semantic-control rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| MAESTRO | 64 | 0 | 100.0% | 0.0% | 0.0% |
| ASAP | 46 | 18 | 71.875% | 12.5% | 0.0% |

ASAP's remaining sample-level rejection classes were aftertouch (8 streams),
SysEx (6), tempo change (5), unsupported timing event (3), meter change (2),
and tempo out of range (1). These counts are overlapping stream classes, not
a mandate to normalize any of them.
