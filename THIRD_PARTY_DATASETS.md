# Third-party datasets

The authoritative machine-readable registry is `configs/sources/registry.json`. It records official source, terms, license evidence, attribution/citation, and the conversion Motherlode performs. License eligibility is source-specific and no deduplication action changes rights.

## User-authorized automated sources

The following official acquisition endpoints were supplied and their terms were
accepted by the operator on 2026-08-21. This authorization allows the tooling
to acquire and process them; it does not alter the upstream licenses, which
remain attached to every provenance edge and are restricted to `RESEARCH_MAX`
where applicable.

| Dataset | Official payload / record | Citation recorded by Motherlode | Conversion |
| --- | --- | --- | --- |
| ComMU | [project archive](https://pozalabs.github.io/ComMU/assets/ComMU.tar) | ComMU, [arXiv:2211.09385](https://arxiv.org/abs/2211.09385) | Original MIDI is indexed; pitched tracks become V1 candidates. |
| MAESTRO v3 | [MIDI archive](https://storage.googleapis.com/magentadata/datasets/maestro/v3.0.0/maestro-v3.0.0-midi.zip), [metadata](https://storage.googleapis.com/magentadata/datasets/maestro/v3.0.0/maestro-v3.0.0.json) | Hawthorne et al. (2019) | Original MIDI is indexed; metadata is stored separately. |
| ASAP | [official repository archive](https://github.com/fosfrancesco/asap-dataset/archive/refs/heads/master.zip) | Foscarin et al. (2020) | Original symbolic files are retained and MIDI is indexed when present. |
| EMOPIA | [official repository archive](https://github.com/annahung31/EMOPIA/archive/refs/heads/main.zip) | Hung et al. (2021) | Original MIDI is indexed; labels remain source-qualified. |
| Groove MIDI | [MIDI-only archive](https://storage.googleapis.com/magentadata/datasets/groove/groove-v1.0.0-midionly.zip) | Gillick et al. (2019) | Original MIDI is retained; percussion is preserved for V2 and excluded from V1 pitched eligibility. |
| GigaMIDI v2 | [payload](https://huggingface.co/datasets/Metacreation/GigaMIDI/resolve/main/Final_GigaMIDI_V2.0_Final.zip), [metadata](https://huggingface.co/datasets/Metacreation/GigaMIDI/resolve/main/Final-Metadata-Extended-GigaMIDI-Dataset-updated.csv) | Lee et al., GigaMIDI, TISMIR 2025 | Payload and source-qualified metadata are downloaded separately. |
| Aria-MIDI | [deduplicated extended payload](https://huggingface.co/datasets/loubb/aria-midi/resolve/main/aria-midi-v1-deduped-ext.tar.gz?download=true) | Bradshaw and Colton, [arXiv:2504.15071](https://arxiv.org/abs/2504.15071) | Original MIDI is indexed; no source provenance is discarded. |
| GuitarSet annotations | [Zenodo annotation archive](https://zenodo.org/records/3371780/files/annotation.zip?download=1) | Xiang et al. (2018) | Recorded as an annotation reference only; it is not treated as complete source payload or V1-eligible data. |

## Reviewed manual-gated sources

The [2026-08-22 license review](docs/license-review-2026-08-22.md) records the
evidence and lane decisions for ATEPP, GiantMIDI-Piano, Los Angeles MIDI,
PiJAMA, MID-FiLD, Pop1K7, Symphony MIDI, and FiloSax. A manual gate means the
tooling will not invent a download URL or bypass an upstream agreement.
