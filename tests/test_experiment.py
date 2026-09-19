"""Offline tests for incremental experiment collection and stores."""

from datetime import date
from datetime import timedelta
from pathlib import Path

import polars as pl

from ana import AnaRequest
from ana import Experiment
from ana import LiveHorizon
from ana import Station
from ana import StationCodes
from ana.events import StorePlanned
from conftest import FakeProvider


def _station(code: str) -> Station:
    return Station(
        code=code,
        name=f"Station {code}",
        latitude=-15,
        longitude=-47,
        station_type="pluviometrica",
    )


def test_fixed_request_reuses_covered_interval(tmp_path: Path) -> None:
    provider = FakeProvider([_station("A")])
    experiment = Experiment(
        name="fixed",
        main=AnaRequest(
            selection=StationCodes(codes=("A",)),
            start=date(2020, 1, 1),
            end=date(2020, 1, 3),
        ),
        downloader=provider,
        root_dir=tmp_path,
    )
    experiment.download()
    experiment.download()
    assert len(provider.calls) == 1
    assert provider.calls[0][1:3] == ("2020-01-01", "2020-01-03")


def test_live_request_skips_recent_mutable_tail(tmp_path: Path) -> None:
    provider = FakeProvider([_station("A")])
    experiment = Experiment(
        name="live",
        main=AnaRequest(
            selection=StationCodes(codes=("A",)),
            start=date.today() - timedelta(days=5),
            end=LiveHorizon.TODAY,
        ),
        downloader=provider,
        root_dir=tmp_path,
    )
    experiment.download()
    first_count = len(provider.calls)
    experiment.download()
    assert len(provider.calls) == first_count


def test_live_request_refreshes_stale_mutable_tail(tmp_path: Path) -> None:
    provider = FakeProvider([_station("A")])
    experiment = Experiment(
        name="live",
        main=AnaRequest(
            selection=StationCodes(codes=("A",)),
            start=date.today() - timedelta(days=5),
            end=LiveHorizon.TODAY,
        ),
        downloader=provider,
        root_dir=tmp_path,
        active_tail_ttl=timedelta(0),
    )
    experiment.download()
    first_count = len(provider.calls)
    experiment.download()
    assert len(provider.calls) == first_count + 1
    assert provider.calls[-1][1] == (date.today() - timedelta(days=1)).isoformat()


def test_store_is_parquet_and_emits_write_event(tmp_path: Path) -> None:
    provider = FakeProvider([_station("A")])
    experiment = Experiment(
        name="store",
        main=AnaRequest(
            selection=StationCodes(codes=("A",)),
            start=date(2020, 1, 1),
            end=date(2020, 1, 1),
        ),
        downloader=provider,
        root_dir=tmp_path,
    )
    experiment.download()
    events = []
    store = experiment.to_parquet(listener=events.append)
    assert store.is_dir()
    assert store == experiment.store_path
    assert store.suffix == ".parquet"
    assert store.parent.name == "store"
    assert store.parent.parent.name == "ana"
    assert experiment.cache_path == tmp_path / ".cache" / "fragments" / "ana" / "v2"
    assert (store.parent / "manifest.json").is_file()
    assert any(isinstance(event, StorePlanned) for event in events)
    observations = experiment.open("observations")
    assert isinstance(observations, pl.DataFrame)
    assert observations.select("station_code").to_series().to_list() == ["A"]
    stations = experiment.open("stations")
    assert isinstance(stations, pl.DataFrame)
    assert stations.select("station_code").to_series().to_list() == ["A"]
