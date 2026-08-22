# License and acquisition review — 2026-08-22

This is a source-specific evidence review, not legal advice. Operator acceptance
of a click-through or disclaimer authorizes acquisition only where the upstream
terms permit it. It does not turn a code license, a paper, or a public link into
a license for an unrelated dataset payload.

## Newly cleared for the research lane

| Source | Evidence | Classification | Acquisition state |
| --- | --- | --- | --- |
| ATEPP | The official README identifies CC BY 4.0 and requires its disclaimer before download. | `ALLOWED` / `RESEARCH_MAX`; attribution retained. | Manual-gated until the authorized payload link is supplied or automatable. |
| GiantMIDI-Piano | The official repository identifies CC BY 4.0 and requires its disclaimer before the stable MIDI download. | `ALLOWED` / `RESEARCH_MAX`; attribution retained. | Manual-gated; no archive is committed or redistributed. |
| Los Angeles MIDI Dataset v4 | The official Hugging Face dataset card labels the payload CC BY-NC-SA 4.0. | `ALLOWED` / `RESEARCH_MAX`; never the permissive/PD lane. | The registry now has the official v4 payload URL; normal resumable staging can acquire it. |

`RESEARCH_MAX` is intentionally conservative for all of these entries. It
preserves the upstream source/license edge and avoids claiming a clean commercial
lane where provenance includes transcriptions or non-commercial terms.

## Excluded pending payload-specific rights

| Source | Finding | Registry action |
| --- | --- | --- |
| PiJAMA | The project links MIDI automatically transcribed from recordings but this review found no dataset-wide license covering that payload. | `UNKNOWN_OR_RESTRICTED`; no acquisition. |
| MID-FiLD | The official AAAI paper describes the dataset but supplies neither a dataset license nor an authorized public payload endpoint. | `UNKNOWN_OR_RESTRICTED`; no acquisition. |
| Pop1K7 | The project code is GPL-3.0, but that is not a demonstrated license for the separately linked MIDI dataset. | `UNKNOWN_OR_RESTRICTED`; no acquisition. |
| Symphony MIDI | The SymphonyNet repository is MIT-licensed code; the project download does not state a dataset-payload license. | `UNKNOWN_OR_RESTRICTED`; no acquisition. |

The same rule continues to protect the remaining uncertain sources: no training
eligibility is created by acceptance of terms where the available evidence does
not actually grant the relevant dataset rights. This is especially important for
collections assembled from commercial recordings, web MIDI, game music, or
third-party backing tracks.

## Terms that require a separate access grant

FiloSax publishes non-commercial research conditions and requires a named
researcher/organisation agreement before the Zenodo download permission is
issued. It also forbids redistribution. Motherlode leaves it manual-gated and
does not store its payload in Git. The current operator acceptance is recorded,
but an upstream access grant and an authorized archive are still required before
the adapter can proceed.

## Implementation consequences

- Each cleared source retains its own license lane; musical deduplication never
  removes conflicting source rights.
- The existing GigaMIDI Lightning-to-R2 transfer is unaffected and remains in
  `RESEARCH_MAX` under its payload-specific CC BY-NC terms.
- No upstream terms were bypassed; no restricted corpus data was committed.
