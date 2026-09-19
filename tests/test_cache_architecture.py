"""Tests for the cache namespace, fingerprint, and manifest contract."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest

from ana.cache import ExperimentNamespace
from ana.cache import StationCache
from ana.cache import StationMeta
from ana.cache import request_fingerprint
from ana.cache import summarize_coverage
from ana.cache import validate_slug
from ana.exceptions import AnaValidationError
from ana.models import AnaRequest
from ana.models import LiveHorizon
from ana.models import Region
from ana.models import StationCodes


if TYPE_CHECKING:
    from pathlib import Path


def _request(day: date = date(2026, 9, 1)) -> AnaRequest:
    return AnaRequest(
        start=day,
        end=day,
        selection=StationCodes(codes=("A",)),
    )


def test_fingerprint_excludes_name_and_tracks_fields() -> None:
    same = request_fingerprint({"main": _request()})
    assert same == request_fingerprint({"main": _request()})
    assert same != request_fingerprint({"main": _request(date(2026, 9, 2))})
    assert same != request_fingerprint({"other": _request()})


def test_live_fingerprint_does_not_depend_on_today() -> None:
    request = AnaRequest(
        start=date(2020, 1, 1),
        end=LiveHorizon.TODAY,
        selection=StationCodes(codes=("1",)),
    )
    assert request_fingerprint({"main": request}) == request_fingerprint(
        {"main": request}
    )


def test_slug_validation() -> None:
    assert validate_slug("rain-brasil") == "rain-brasil"
    for bad in ("", " ", "../escape", "a/b", ".hidden"):
        with pytest.raises(AnaValidationError):
            validate_slug(bad)


def test_namespace_paths(tmp_path: Path) -> None:
    namespace = ExperimentNamespace(tmp_path, "ana", "rain")
    assert namespace.pool_dir == tmp_path / ".cache" / "fragments" / "ana" / "v2"
    assert namespace.store_path("abc") == (
        tmp_path / ".cache" / "stores" / "ana" / "rain" / "abc.parquet"
    )


def test_pool_is_shared_across_namespaces(tmp_path: Path) -> None:
    first = ExperimentNamespace(tmp_path, "ana", "rain")
    second = ExperimentNamespace(tmp_path, "ana", "level")
    assert first.pool_dir == second.pool_dir
    assert first.data_dir != second.data_dir


def test_station_cache_is_rooted_in_the_pool(tmp_path: Path) -> None:
    namespace = ExperimentNamespace(tmp_path, "ana", "rain")
    cache = StationCache(namespace.pool_dir / "stations")
    cache.save_meta(StationMeta.create("A", "chuva"))
    assert cache.load_meta("chuva", "A") == StationMeta.create("A", "chuva")


def test_manifest_round_trip(tmp_path: Path) -> None:
    namespace = ExperimentNamespace(tmp_path, "ana", "rain")
    assert namespace.load_manifest() is None
    namespace.record_store(
        "fp1",
        requests={"main": {"type": "AnaRequest"}},
        coverage={"time": ["2026-09-01", "2026-09-01"]},
        provenance={"0": "single"},
        now="2026-09-18T12:00:00+00:00",
    )
    manifest = namespace.load_manifest()
    assert manifest is not None
    assert manifest.current == "fp1"
    assert manifest.stores["fp1"].coverage["time"] == ["2026-09-01", "2026-09-01"]
    assert manifest.stores["fp1"].provenance == {"0": "single"}


def test_coverage_reports_stations_and_grid() -> None:
    requests = {
        "codes": AnaRequest(
            start=date(2026, 9, 1),
            end=date(2026, 9, 2),
            selection=StationCodes(codes=("A", "B")),
        ),
        "region": AnaRequest(
            start=date(2026, 9, 1),
            end=date(2026, 9, 3),
            selection=Region.SP,
        ),
    }
    coverage = summarize_coverage(requests)
    assert coverage["time"] == ["2026-09-01", "2026-09-03"]
    assert coverage["variables"] == ["chuva"]
    assert coverage["stations"] == ["A", "B", "region:SP"]
    assert coverage["grid"] == "point:stations"
