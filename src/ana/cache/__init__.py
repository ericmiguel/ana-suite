"""Experiment cache identity, namespaces, manifests, coverage, and provenance."""

from __future__ import annotations

from pathlib import Path

from ana.cache.coverage import GRID_SIGNATURE
from ana.cache.coverage import summarize_coverage
from ana.cache.identity import CACHE_SCHEMA_VERSION
from ana.cache.identity import CACHE_SOURCE
from ana.cache.identity import experiment_cache_key
from ana.cache.identity import normalize_dataclass
from ana.cache.identity import normalize_value
from ana.cache.identity import request_fingerprint
from ana.cache.manifest import ExperimentManifest
from ana.cache.manifest import StoreRecord
from ana.cache.namespace import ExperimentNamespace
from ana.cache.namespace import default_namespace
from ana.cache.namespace import validate_slug
from ana.cache.provenance import LEGEND
from ana.cache.provenance import SINGLE
from ana.cache.provenance import legend_payload
from ana.cache.station import StationCache
from ana.cache.station import StationMeta


def experiment_cache_dir(cache_root: Path, cache_key: str) -> Path:
    """Return the legacy isolated cache directory for one experiment."""
    return Path(cache_root) / CACHE_SOURCE / cache_key


def experiment_store_path(data_root: Path, cache_key: str) -> Path:
    """Return the legacy canonical Parquet directory for one experiment."""
    return Path(data_root) / CACHE_SOURCE / f"{cache_key}.parquet"


__all__ = [
    "CACHE_SCHEMA_VERSION",
    "CACHE_SOURCE",
    "GRID_SIGNATURE",
    "LEGEND",
    "SINGLE",
    "ExperimentManifest",
    "ExperimentNamespace",
    "StationCache",
    "StationMeta",
    "StoreRecord",
    "default_namespace",
    "experiment_cache_dir",
    "experiment_cache_key",
    "experiment_store_path",
    "legend_payload",
    "normalize_dataclass",
    "normalize_value",
    "request_fingerprint",
    "summarize_coverage",
    "validate_slug",
]
