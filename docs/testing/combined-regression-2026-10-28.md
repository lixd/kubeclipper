# Combined OCI regression: 2026-10-28

This record covers a targeted three-node regression of the combined F01, F02,
F03, F04, F05 and F07 fixes. It is not the full OCI release qualification.

The backend source tested was an uncommitted candidate with source fingerprint
`e2bb2027431ed8755d5a8e1f744c2dc0aa20474cefeb135bdd1aca31c9f020fd`.
Its 759-file source manifest matched the subsequently created backend commit
`b4b8f9bdd1cd00a9f0432c4c54cb274d98ea75ab`. The tests did not use a published
artifact built from that Git commit; later commits were not included in this
live run.

- F01: a valid mirror with a repository path was accepted on three agents;
  invalid input was rejected before remote deployment.
- F07: default Registry creation, authenticated TLS, offline cluster creation
  and addition of a third node succeeded. Pulls used the repository prefix;
  the CRI connection used the registry host.
- F02: concurrent keyed retries reused a single backup and Operation. Backup,
  restart recovery, restore and replay after deletion were exercised.
- F03: successful updates returned the cluster to Running; a timed-out update
  produced UpdateFailed, rejected a conflicting update, and succeeded on retry.
- F05: a missing backup point returned 404; an existing point returned 200.
- F04: the UI displayed a real Task failure independently of its log body and
  did not carry the old failure into a successful retry. The narrow viewport
  did not establish desktop-width readability or complete UI coverage.

Unit tests and six critical-package race checks passed. The race count includes
subtests and overlaps the earlier unit run. After testing, the original platform
binaries and configuration were restored, all three platform nodes were Ready,
and `kcctl doctor` reported 25 passed, zero warnings and zero failures. The
seeded default Registry was recreated, so its resource UID changed even though
its previous fields were restored.

The 192-case ledger recorded 10 passed, 3 partial, 177 pending and 2 out of
scope for this run. Invalid component `timeout=45` returned 500 instead of 400;
that issue remained unfixed. Release/download qualification and the complete
UI and topology matrices were not run. Passing historical results and tests
against a different source revision must not be counted as current proof.
