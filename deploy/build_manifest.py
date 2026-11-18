#!/usr/bin/env python3
"""Build the v2 digest manifest for the forced-command deploy.

Inputs (relative to the repository root):

- ``dist/*.txt`` from the build and companions jobs, one ``<key> <ref>`` per
  file, where ref is ``repo@sha256:<64 lowercase hex>``;
- ``deploy/compose/docker-compose.yml`` embedded into ``config.compose_b64``;
- ``deploy/compose/env.required`` copied into ``config.required_env``.

Output: ``manifest.b64`` (unpadded base64url) plus a summary on stdout.
Contract: arch2debian/docs/deployment-contract.md.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

DIST_DIR = Path("dist")
COMPOSE_FILE = Path("deploy/compose/docker-compose.yml")
ENV_REQUIRED_FILE = Path("deploy/compose/env.required")
OUT_FILE = Path("manifest.b64")

IMAGE_KEYS = frozenset({"manager", "acestream", "acexy"})
MANIFEST_KEYS = frozenset({"app", "config", "images", "release"})
CONFIG_KEYS = frozenset({"compose_b64", "compose_sha256", "required_env"})
MAX_COMPOSE_BYTES = 256 * 1024

ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
COMPOSE_VAR = re.compile(r"\$\{([A-Z_][A-Z0-9_]*):\?")
DIGEST_REF = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def load_images(dist: Path) -> dict[str, str]:
    images: dict[str, str] = {}
    for path in sorted(dist.glob("*.txt")):
        line = path.read_text(encoding="utf-8").strip()
        fields = line.split(None, 1)
        if len(fields) != 2:
            fail(f"{path}: expected '<key> <ref>', got {line!r}")
        key, ref = fields
        if key in images:
            fail(f"{path}: duplicate image key {key!r}")
        if not DIGEST_REF.match(ref):
            fail(f"{path}: not a digest reference: {ref!r}")
        images[key] = ref
    if set(images) != IMAGE_KEYS:
        fail(f"image key set mismatch: {sorted(images)} != {sorted(IMAGE_KEYS)}")
    return images


def load_required_env(path: Path) -> list[str]:
    names: list[str] = []
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        name = raw.strip()
        if not name or name.startswith("#"):
            continue
        if not ENV_NAME.match(name):
            fail(f"{path}:{number}: invalid variable name {name!r}")
        names.append(name)
    if not names:
        fail(f"{path}: no variables")
    if len(set(names)) != len(names):
        fail(f"{path}: duplicate variable names")
    if names != sorted(names):
        fail(f"{path}: variable names must be sorted")
    return names


def load_compose(path: Path) -> bytes:
    content = path.read_bytes()
    if len(content) > MAX_COMPOSE_BYTES:
        fail(f"{path}: {len(content)} bytes exceeds {MAX_COMPOSE_BYTES}")
    return content


def check_compose_env(content: bytes, required_env: list[str]) -> None:
    """env.required is exactly the non-image variables the compose needs."""
    referenced = set(COMPOSE_VAR.findall(content.decode("utf-8")))
    host_vars = {name for name in referenced if not name.endswith("_IMAGE_REF")}
    if host_vars != set(required_env):
        missing = sorted(host_vars - set(required_env))
        extra = sorted(set(required_env) - host_vars)
        fail(f"compose/env.required mismatch: missing={missing} extra={extra}")


def build_manifest(
    app: str,
    release: str,
    images: dict[str, str],
    content: bytes,
    required_env: list[str],
) -> dict[str, Any]:
    config = {
        "compose_b64": base64.b64encode(content).decode("ascii"),
        "compose_sha256": hashlib.sha256(content).hexdigest(),
        "required_env": required_env,
    }
    manifest: dict[str, Any] = {
        "app": app,
        "config": config,
        "images": images,
        "release": release,
    }
    if set(manifest) != MANIFEST_KEYS:
        fail(f"manifest key set mismatch: {sorted(manifest)}")
    if set(config) != CONFIG_KEYS:
        fail(f"config key set mismatch: {sorted(config)}")
    return manifest


def main() -> int:
    app = os.environ.get("APP", "").strip()
    release = os.environ.get("RELEASE", "").strip()
    if not app or not release:
        fail("APP and RELEASE must be set")

    images = load_images(DIST_DIR)
    content = load_compose(COMPOSE_FILE)
    required_env = load_required_env(ENV_REQUIRED_FILE)
    check_compose_env(content, required_env)
    manifest = build_manifest(app, release, images, content, required_env)

    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    OUT_FILE.write_text(encoded, encoding="ascii")

    config = dict(manifest["config"])
    summary = {
        **manifest,
        "config": {**config, "compose_b64": f"<{len(content)} bytes>"},
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"compose bytes: {len(content)} (limit {MAX_COMPOSE_BYTES})")
    print(f"compose sha256: {config['compose_sha256']}")
    print(f"manifest bytes: {len(raw)} base64url chars: {len(encoded)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
