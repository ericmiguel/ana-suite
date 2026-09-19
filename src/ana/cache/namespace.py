"""Experiment namespaces: slug validation and atomic manifest persistence."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from ana.cache.identity import CACHE_SCHEMA_VERSION
from ana.cache.identity import CACHE_SOURCE
from ana.cache.manifest import ExperimentManifest
from ana.cache.manifest import StoreRecord
from ana.exceptions import AnaValidationError


if TYPE_CHECKING:
    from collections.abc import Mapping


SLUG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

CACHE_ROOT = ".cache"
FRAGMENT_ROOT = "fragments"
STORE_ROOT = "stores"


def validate_slug(name: str) -> str:
    """Return ``name`` when it is a safe path segment.

    Raises
    ------
    AnaValidationError
        When the name is empty or would not survive as a single directory.
    """
    if not name or not SLUG_PATTERN.match(name):
        raise AnaValidationError(
            "Experiment name must start with an alphanumeric character and "
            "contain only letters, digits, dots, underscores, or hyphens."
        )
    return name


class ExperimentNamespace:
    """The data namespace of one experiment.

    Fragments live in a source-global pool shared by every namespace; only the
    stores and their manifest are namespace-owned.

    Parameters
    ----------
    root : pathlib.Path
        Project root holding the ``.cache`` tree.
    source : str
        Package source label, e.g. ``"ana"``.
    name : str
        Experiment name, validated as a path-safe slug.
    """

    def __init__(self, root: Path, source: str, name: str) -> None:
        self.name = validate_slug(name)
        self.source = source
        self.root = Path(root)
        self.pool_dir = (
            self.root / CACHE_ROOT / FRAGMENT_ROOT / source / f"v{CACHE_SCHEMA_VERSION}"
        )
        self.data_dir = self.root / CACHE_ROOT / STORE_ROOT / source / self.name
        self.manifest_path = self.data_dir / "manifest.json"

    def store_path(self, fingerprint: str) -> Path:
        """Return the store path for one request fingerprint."""
        return self.data_dir / f"{fingerprint}.parquet"

    def load_manifest(self) -> ExperimentManifest | None:
        """Return the persisted manifest, or ``None`` when absent or stale."""
        if not self.manifest_path.is_file():
            return None
        payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest = ExperimentManifest.from_payload(payload)
        if manifest.schema != CACHE_SCHEMA_VERSION:
            return None
        return manifest

    def save_manifest(self, manifest: ExperimentManifest) -> None:
        """Write the manifest atomically."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.manifest_path.with_name(f".{self.manifest_path.name}.part")
        temporary.write_text(
            json.dumps(manifest.to_payload(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self.manifest_path)

    def record_store(
        self,
        fingerprint: str,
        *,
        requests: Mapping[str, object],
        coverage: Mapping[str, object],
        provenance: Mapping[str, str],
        now: str,
    ) -> ExperimentManifest:
        """Record a built store, creating the manifest when needed."""
        manifest = self.load_manifest() or ExperimentManifest(
            schema=CACHE_SCHEMA_VERSION,
            source=self.source,
            experiment=self.name,
        )
        manifest.stores[fingerprint] = StoreRecord(
            fingerprint=fingerprint,
            requests=dict(requests),
            coverage=dict(coverage),
            provenance=dict(provenance),
            built_at=now,
            last_used_at=now,
        )
        manifest.current = fingerprint
        self.save_manifest(manifest)
        return manifest


def default_namespace(root: Path, name: str) -> ExperimentNamespace:
    """Return the namespace of an ANA experiment under ``root``."""
    return ExperimentNamespace(root, CACHE_SOURCE, name)
