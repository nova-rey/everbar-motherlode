# V2 development preview authority decision

Date: 2026-08-30

The historical `ev1-preview-clean-30k-v1` authority remains unavailable and is
not reconstructed or renamed. The blocker specialist returned
`PROPOSED_UNBLOCK` and identified the provenance-rich POP909 POC authority as
the minimum trustworthy local basis for bounded V2 development. The larger
runtime database is not used as sole authority because its source-piece and
source-track closure is absent.

Decision: Option C.

New authority: `ev2-dev-preview-pop909-poc-v1`, built from accepted POP909 POC
receipts, the read-only canonical SQLite, persisted PerTok tokenizations, and
source indexes. It has 1,611 unique canonical streams, 832 source families,
1,598 packable streams, cap 160, and 121,412 windows. Source-family splits are
train 62,553, validation 7,598, test 6,282. Membership identity is
`d95318383f819a84efb00e10c715e050d5abae4e15d5282f5195c90f6e1feeb1`, semantic
identity is `236157589a24f8e36efb8085b6f18c37dc41ed6bcbe70fd63ea042c96d1fc51c`,
and the root manifest file SHA-256 is
`4bc69871a32a5f3faa453eec3712d1b0b89f57c993095ea74d6733ac966a2e3c`.

This is a development authority only. It does not reproduce the historical
V1 membership, manifest, semantic hash, or packed-array identity.
