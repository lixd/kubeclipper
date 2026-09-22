# Release acceptance records

Each stable release must publish exactly the artifact set that was qualified
by CI **and** accepted by a recorded testing round. The release workflow's
`release-gate` job enforces this mechanically via
`scripts/open-packaging/release-gate.sh`.

## Record file

One YAML file per accepted candidate, committed to this directory:

    docs/testing/acceptance/<candidate-sha>.yaml

`<candidate-sha>` is the full 40-hex git commit of the candidate that was
tested. The file is flat with these keys:

```yaml
# The exact commit the qualification run and acceptance round tested.
candidate_sha: e9d9afe0f2c1aabbccddeeff0011223344556677
# passed | failed. The gate blocks on anything other than `passed`.
result: passed
# sha256 of the qualification run's `oci-release-manifest-<run_id>` artifact
# (the release-manifest.yaml uploaded by publish-oci-qualification.yml).
# This pins the release to the exact bytes that were qualified.
release_manifest_sha256: 64-hex-sha256
# Optional. Must match the released tag when present.
release_tag: v2.0.3
# Optional provenance for future audits.
round: R14
notes: >-
  Free-form summary of the acceptance round: environment, scope, evidence
  references. Never record credentials, tokens or kubeconfigs here.
```

## What the gate verifies at release time

For a stable `vX.Y.Z` tag on commit `<sha>`, before `publish` and `build-cli`
run, the gate blocks unless:

1. a **successful** `publish-oci-qualification.yml` run exists for commit
   `<sha>` (resolved via the Actions API, head_sha + status filters);
2. that run's release manifest is a `ReleaseManifest` for the same version
   whose `metadata.sourceRevision` and bootstrap/kubeclipper artifact
   revisions equal `<sha>`;
3. `docs/testing/acceptance/<sha>.yaml` exists, records `result: passed`,
   and its `release_manifest_sha256` equals the sha256 of the downloaded
   qualification manifest;
4. the release tag still points at `<sha>` (a moved tag aborts the release).

Artifact digests themselves are verified inside the qualification run
(`verify-release-manifest.sh`) and again after publishing (release `manifest`
job), so the gate deliberately checks the *binding* — same commit, same
manifest bytes, recorded acceptance — rather than re-walking the registry.

## Process

1. Trigger `publish-oci-qualification.yml` (`oci-qualification-<sha>` tag) on
   the candidate commit and wait for success.
2. Run the acceptance round on that exact build; record its outcome here.
3. Tag the stable release on the same commit. The `release` workflow gate
   then passes only if steps 1-2 were done for this commit.

A `failed` record blocks release; fix, re-qualify the new commit, and add a
fresh record for it.
