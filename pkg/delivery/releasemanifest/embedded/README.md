Release and qualification CLI builds copy the verified `release-manifest.yaml`
into this directory before compiling kcctl. Go embeds that inventory in the
standalone binary. The generated file is ignored by Git.

Package publication runs first so the inventory contains final image digests.
Bootstrap package builds precede this step to avoid including their own digest
in the kcctl binary inside the package.

Source builds without the generated inventory use an explicit `--manifest`.
