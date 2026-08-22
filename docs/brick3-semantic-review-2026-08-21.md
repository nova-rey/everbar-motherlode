# Brick 3 semantic review — MAESTRO and ASAP

## Purpose

This is a decision brief for the Everbar development thread. It records what
Motherlode observed without changing Brick 3 or silently changing source MIDI.
The corpus authority remains the pinned `dreamstream-everbar` Brick 3 policy.

## Finding 1: invalid Lightning execution receipts

The initial Lightning worker configuration named the workstation-only checkout
`/home/nyx/dreamstream-everbar`. On Lightning the pinned checkout is
`/teamspace/studios/this_studio/dreamstream-everbar`; the initial receipt
diagnostic was therefore `No such file or directory`, not a Brick 3 policy
decision.

This deployment error has been fixed by the explicit `EVERBAR_CHECKOUT`
boundary in Motherlode. Any receipt carrying that diagnostic is invalid for
corpus-policy analysis and is being regenerated. This finding requires no
change to Brick 3 semantics.

## Finding 2: MAESTRO controller semantics

A bounded MAESTRO V1-derived stream was evaluated directly against the pinned
remote authority. Brick 3 returned `REJECT_SEMANTIC_CONTROL_CHANGE` with
4,911 reason instances in that sample. The reported controls were:

| MIDI controller | Conventional meaning | Brick 3 result |
| --- | --- | --- |
| CC64 | sustain pedal | semantic control change rejection |
| CC66 | sostenuto pedal | semantic control change rejection |
| CC67 | soft pedal | semantic control change rejection |

These are musically meaningful piano-performance controls. Removing them in
Motherlode would change the source semantics; keeping them is incompatible
with the current policy. The single-sample count is evidence of the issue, not
a dataset-wide rejection rate.

## Finding 3: ASAP controller semantics and zero-duration note

A bounded ASAP V1-derived stream was evaluated directly against the same
authority. Brick 3 returned:

- `REJECT_SEMANTIC_CONTROL_CHANGE` for CC64 sustain-pedal changes; and
- `REJECT_INVALID_NOTE` for a note with `pitch=37`, `tick=86400`, and
  `duration=0`.

The zero-duration event may be an encoding artifact or may require an explicit
interpretation rule. Motherlode must not discard or repair it implicitly.
Again, this is a sample-level finding and not a claimed corpus-wide rate.

## Decision record

Everbar accepted `performance-flattening-v1` upstream of Brick 3. Its exact
rules and receipt format are documented in
[performance-flattening-v1.md](performance-flattening-v1.md). The original
source and the unflattened derived candidate remain preserved.

This decision resolves CC64, CC66, CC67, CC121 Reset All Controllers, and
verified zero-duration note pairs. CC121 is consumed at its source tick only
after resetting the associated held-controller state; it is not merely
stripped. This does not relax Brick 3.

## Remaining decisions required from Everbar

The converted ASAP sample exposes separate CC121 and unsupported-meter
rejections. Any treatment of those conditions needs its own reviewed policy;
Motherlode continues to preserve the source, conversion receipt, and Brick 3
diagnostics without admitting rejected candidates into a training lane.
