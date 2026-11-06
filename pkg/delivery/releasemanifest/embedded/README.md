`release-manifest.yaml` is the checked-in tag inventory for the release. Every
kcctl build embeds it, including the copy inside the bootstrap package.

Regenerate it from the complete resource build using
`scripts/open-packaging/generate-release-manifest.sh --include-bootstrap --arch all`
and the desired `--output` path. Qualification compares the assembled inventory
with this file and verifies every published tag and platform. Release builds
bind their global registry prefix through the existing source configuration.

The inventory records versions and tags. OCI content digests and offline
checksums are computed and verified during transport.
