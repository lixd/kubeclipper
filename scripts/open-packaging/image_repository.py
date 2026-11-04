#!/usr/bin/env python3
"""Map upstream runtime images to the same flat repositories used by agents."""

import json
from pathlib import Path
import sys

ALIASES = json.loads((Path(__file__).resolve().parents[2] /
                      "pkg/delivery/registry/image_repositories.json").read_text())


def target_image(source, registry):
    if not registry:
        return ""
    repository, sep, digest = source.partition("@")
    suffix = "@" + digest if sep else ""
    if ":" in repository.rsplit("/", 1)[-1]:
        repository, tag = repository.rsplit(":", 1)
        suffix = ":" + tag + suffix
    first = repository.split("/", 1)[0]
    explicit_registry = "/" in repository and ("." in first or ":" in first or first == "localhost")
    if explicit_registry:
        repository = repository.split("/", 1)[1]
    if "/" not in repository and (not explicit_registry or first in ("docker.io", "index.docker.io")):
        repository = "library/" + repository
    if "/" not in registry.rstrip("/"):
        repository = "coredns" if repository == "coredns/coredns" else repository
        return registry.rstrip("/") + "/" + repository + suffix
    flat = ALIASES.get(repository, repository.replace("/", "-"))
    return registry.rstrip("/") + "/" + flat + suffix


if __name__ == "__main__":
    print(target_image(sys.argv[1], sys.argv[2]))
