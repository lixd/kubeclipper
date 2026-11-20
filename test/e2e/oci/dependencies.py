#!/usr/bin/env python3
"""Select the published Kubernetes dependencies for the OCI AIO upgrade check."""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts" / "open-packaging"))
from image_repository import target_image


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: dependencies.py RELEASE_MANIFEST REGISTRY OUTPUT")
    registry = sys.argv[2].rstrip("/")
    with open(sys.argv[1], encoding="utf-8") as source:
        manifest = yaml.safe_load(source)
    if manifest.get("kind") != "ReleaseManifest":
        raise SystemExit("expected a KubeClipper ReleaseManifest")

    packages = {
        ("k8s", "k8s", "v1.36.4"),
        ("k8s", "k8s", "v1.37.0"),
        ("cri", "containerd", "2.2.4"),
        ("cni", "calico", "v3.31.5"),
        ("k8s-extension", "k8s-extension", "v1"),
    }
    runtime_only = {("kc-runtime", "kc-runtime", "v2.0.0")}
    found_packages = set()
    found_images = set()
    selected = []
    for artifact in manifest["artifacts"]:
        component = artifact["component"]
        identity = (component["kind"], component["name"], component["version"])
        if "linux/amd64" not in artifact["platforms"]:
            continue
        if artifact["type"] == "package-image" and identity in packages:
            found_packages.add(identity)
            selected.append(artifact)
        if artifact["type"] == "runtime-image" and identity in packages | runtime_only:
            found_images.add(identity)
            selected.append(artifact)

    missing_packages = packages - found_packages
    missing_images = (packages | runtime_only) - {("cri", "containerd", "2.2.4")} - found_images
    if missing_packages or missing_images:
        raise SystemExit(
            f"incomplete amd64 dependencies, packages={sorted(missing_packages)}, "
            f"image sets={sorted(missing_images)}"
        )

    with open(sys.argv[3], "w", encoding="utf-8") as target:
        for artifact in selected:
            source = artifact["source"]
            if artifact.get("digest") and "@" not in source:
                source += "@" + artifact["digest"]
            if artifact["type"] == "runtime-image":
                destination = target_image(artifact["upstream"], registry)
            else:
                destination = registry + "/" + artifact["target"]
            print(source, destination, sep="\t", file=target)


if __name__ == "__main__":
    main()
