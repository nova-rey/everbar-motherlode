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

## Decision required from Everbar

Choose and version one policy direction before Motherlode makes either dataset
training eligible:

1. Keep the current policy. MAESTRO/ASAP streams with these events remain
   rejected; report their acceptance rates after a clean rerun.
2. Extend upstream Brick 3 to represent or explicitly normalize the relevant
   controller semantics and/or zero-duration events. This must be an Everbar
   policy/version decision with tests and a new policy hash.
3. Define a source-conversion policy that removes or transforms the events.
   This is a semantic change, must be explicit and receipted, and must not be
   presented as original MIDI.

Until then, Motherlode preserves original bytes, derived candidates, and
rejection diagnostics, but does not admit rejected candidates into a training
lane.
