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
- counts for CC64/CC66 render operations, CC67 removals, zero-duration drops,
  and any end-of-track note-off flushes; and
- a receipt hash.

The flattened MIDI is written beneath `derived/<dataset>/prebrick3/`. It is a
derived conversion, never an original source file.

## Bounded validation evidence

The original MAESTRO sample rejected for CC64/CC66/CC67 was accepted after
conversion with no controller or invalid-note reason codes. The original ASAP
sample no longer produced `REJECT_INVALID_NOTE` after its zero-duration no-op
was removed. Its converted sample still produced unrelated
`REJECT_SEMANTIC_CONTROL_CHANGE` for CC121 and `REJECT_UNSUPPORTED_METER`; this
policy does not alter those events.
