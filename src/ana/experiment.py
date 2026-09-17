"""Experiment orchestration for incremental ANA station collection."""

import datetime as dt
import logging
from collections.abc import Iterable
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from typing import cast

import polars as pl

from ana.cache import StationCache
from ana.cache import StationMeta
from ana.cache import experiment_cache_dir
from ana.cache import experiment_cache_key
from ana.cache import experiment_store_path
from ana.events import ItemWritten
from ana.events import PipelineListener
from ana.events import RequestPlanned
from ana.events import StationResolved
from ana.events import StorePlanned
from ana.exceptions import AnaValidationError
from ana.intervals import find_fetch_targets
from ana.intervals import merge_checked_ranges
from ana.models import AnaRequest
from ana.models import Station
from ana.retrieval import AnaClient
from ana.retrieval import AnaProvider
from ana.retrieval import FetchResult
from ana.root import resolve_project_root
from ana.selection import select_stations
from ana.store import OBSERVATIONS_TABLE
from ana.store import STATIONS_TABLE
from ana.store import read_table
from ana.store import write_store


LOGGER = logging.getLogger(__name__)
DEFAULT_TTL_SCHEDULE = ((30, 1), (365, 3), (99999, 7))
DEFAULT_UNKNOWN_TTL_DAYS = 30
DEFAULT_INVENTORY_TTL_DAYS = 7
MAX_CONSECUTIVE_ERRORS = 3
ERROR_COOLDOWN = dt.timedelta(days=1)


class Experiment:
    """Collect named ANA requests into an incremental Parquet experiment."""

    def __init__(
        self,
        *,
        name: str,
        downloader: AnaProvider | None = None,
        root_dir: Path | None = None,
        inventory_ttl_days: int = DEFAULT_INVENTORY_TTL_DAYS,
        unknown_ttl_days: int = DEFAULT_UNKNOWN_TTL_DAYS,
        ttl_schedule: tuple[tuple[int, int], ...] = DEFAULT_TTL_SCHEDULE,
        **requests: AnaRequest,
    ) -> None:
        if not name.strip():
            raise AnaValidationError("Experiment name cannot be empty.")
        if not requests:
            raise AnaValidationError("At least one named request is required.")
        if any(not isinstance(request, AnaRequest) for request in requests.values()):
            raise AnaValidationError("Experiment requests must be AnaRequest.")
        self.name = name
        self.requests = dict(requests)
        self.root_dir = resolve_project_root(root_dir)
        self._cache_key = experiment_cache_key(name, self.requests)
        self.cache = StationCache(
            experiment_cache_dir(self.root_dir / ".cache", self._cache_key)
        )
        self.downloader = downloader or AnaClient()
        self.inventory_ttl_days = inventory_ttl_days
        self.unknown_ttl_days = unknown_ttl_days
        self.ttl_schedule = ttl_schedule
        self._selected: dict[str, list[Station]] = {}
        self._cache_changed = False
        self._downloaded = False
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @property
    def cache_key(self) -> str:
        """Return the isolated experiment cache key."""
        return self._cache_key

    @property
    def cache_path(self) -> Path:
        """Return the isolated incremental cache directory."""
        return self.cache.path

    @property
    def store_path(self) -> Path:
        """Return the canonical Parquet store path."""
        return experiment_store_path(self.root_dir / "data", self._cache_key)

    def download(
        self,
        *,
        refresh: bool = False,
        update_active: bool = True,
        max_workers: int = 5,
        listener: PipelineListener | None = None,
    ) -> tuple[Path, ...]:
        """Incrementally collect all requests and return cached station files."""
        self._cache_changed = False
        paths: set[Path] = set()
        for request_name, request in self.requests.items():
            stations = self._stations(request)
            self._selected[request_name] = stations
            if listener is not None:
                listener(RequestPlanned(name=request_name, stations=len(stations)))
            tasks = self._tasks(request, stations, refresh, update_active)
            self._run_tasks(
                request_name,
                request,
                tasks,
                max_workers=max_workers,
                listener=listener,
                paths=paths,
            )
        self._downloaded = True
        return tuple(sorted(paths))

    def to_parquet(
        self,
        *,
        overwrite: bool = False,
        listener: PipelineListener | None = None,
    ) -> Path:
        """Materialize current cache data into the canonical Parquet store."""
        if not self._downloaded:
            raise RuntimeError("Call download() before to_parquet().")
        if self.store_path.exists() and not overwrite and not self._cache_changed:
            return self.store_path
        stations = _unique_stations(self._selected.values())
        observations = self._observations(stations)
        if listener is not None:
            listener(StorePlanned(items=2))
        destination = write_store(
            self.store_path,
            _stations_frame(stations),
            observations,
            overwrite=overwrite or self._cache_changed,
        )
        self._cache_changed = False
        if listener is not None:
            listener(ItemWritten(description=str(destination)))
        return destination

    def open(
        self,
        table: str | None = None,
        *,
        station_code: str | None = None,
        variable: str | None = None,
    ) -> pl.DataFrame | dict[str, pl.DataFrame]:
        """Read one canonical table, or both tables when no table is given."""
        store = self.store_path
        if table is None:
            return {
                STATIONS_TABLE: read_table(store, STATIONS_TABLE),
                OBSERVATIONS_TABLE: read_table(
                    store,
                    OBSERVATIONS_TABLE,
                    station_code=station_code,
                    variable=variable,
                ),
            }
        return read_table(
            store,
            table,
            station_code=station_code,
            variable=variable,
        )

    def scan_catalog(
        self,
        stations: Iterable[Station],
        *,
        variable: str = "chuva",
        probe_days: int = 30,
        max_workers: int = 20,
        listener: PipelineListener | None = None,
    ) -> dict[str, StationMeta]:
        """Probe a recent window to update station availability metadata."""
        today = dt.date.today()
        request = AnaRequest(
            start=today - dt.timedelta(days=probe_days),
            end=today,
            variable=variable,
        )
        tasks = self._tasks(request, list(stations), False, False)
        paths: set[Path] = set()
        self._run_tasks(
            "catalog",
            request,
            tasks,
            max_workers=max_workers,
            listener=listener,
            paths=paths,
        )
        return {
            station.code: self.cache.load_meta(variable, station.code)
            or StationMeta.create(station.code, variable)
            for station in stations
        }

    def _stations(self, request: AnaRequest) -> list[Station]:
        inventory = self.cache.load_inventory()
        age = self.cache.inventory_age_days()
        if inventory is None or age is None or age > self.inventory_ttl_days:
            inventory = self.downloader.fetch_inventory()
            self.cache.save_inventory(inventory)
            self._cache_changed = True
        selected = select_stations(inventory, request.selection)
        if request.station_type is not None:
            selected = [
                station
                for station in selected
                if station.station_type == request.station_type_name
            ]
        return selected

    def _tasks(
        self,
        request: AnaRequest,
        stations: list[Station],
        refresh: bool,
        update_active: bool,
    ) -> list[tuple[Station, str, str]]:
        result: list[tuple[Station, str, str]] = []
        start, end = request.start.isoformat(), request.resolved_end.isoformat()
        for station in stations:
            meta = self.cache.load_meta(
                request.variable_name, station.code
            ) or StationMeta.create(station.code, request.variable_name)
            if self._cooldown(meta):
                continue
            if refresh:
                result.append((station, start, end))
                continue
            if update_active and meta.status == "active" and request.is_live:
                mutable_start = max(
                    request.start,
                    request.resolved_end - dt.timedelta(days=1),
                )
                result.append((station, mutable_start.isoformat(), end))
                continue
            ttl = self._ttl(meta)
            targets = find_fetch_targets(
                meta.checked_ranges, start, end, meta.last_checked, ttl
            )
            if targets:
                result.append(
                    (
                        station,
                        min(item[0] for item in targets),
                        max(item[1] for item in targets),
                    )
                )
        return result

    def _run_tasks(
        self,
        request_name: str,
        request: AnaRequest,
        tasks: list[tuple[Station, str, str]],
        *,
        max_workers: int,
        listener: PipelineListener | None,
        paths: set[Path],
    ) -> None:
        if not tasks:
            return
        futures: dict[
            Future[tuple[str, pl.DataFrame | None]], tuple[Station, str, str]
        ] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for station, start, end in tasks:
                futures[
                    pool.submit(
                        self.downloader.fetch_series,
                        station,
                        start,
                        end,
                        request.variable_name,
                    )
                ] = (station, start, end)
            for future in as_completed(futures):
                station, start, end = futures[future]
                result, frame = future.result()
                path = self._process_result(
                    station, request.variable_name, result, frame, start, end
                )
                if path is not None:
                    paths.add(path)
                if listener is not None:
                    listener(
                        StationResolved(
                            request=request_name, station=station.code, path=path
                        )
                    )

    def _process_result(
        self,
        station: Station,
        variable: str,
        result: str,
        frame: pl.DataFrame | None,
        start: str,
        end: str,
    ) -> Path | None:
        meta = self.cache.load_meta(variable, station.code) or StationMeta.create(
            station.code, variable
        )
        now = dt.datetime.now().isoformat()
        if result == FetchResult.HAS_DATA and frame is not None:
            frame = frame.with_columns(pl.lit(station.code).alias("station_code"))
            self.cache.merge_data(variable, station.code, frame)
            meta.status = "active"
            meta.consecutive_errors = 0
            meta.known_data_start = _min_date(
                meta.known_data_start,
                cast("dt.datetime | None", frame["datetime"].min()),
            )
            meta.known_data_end = _max_date(
                meta.known_data_end, cast("dt.datetime | None", frame["datetime"].max())
            )
            self._cache_changed = True
            kind = "has_data"
        elif result == FetchResult.EMPTY:
            meta.status = "inactive"
            meta.consecutive_errors = 0
            kind = "empty"
        else:
            meta.consecutive_errors += 1
            if meta.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                meta.status = "error"
            kind = "error"
        if result != FetchResult.ERROR:
            meta.checked_ranges = merge_checked_ranges(
                [*meta.checked_ranges, (start, end, kind)]
            )
        meta.last_checked = now
        self.cache.save_meta(meta)
        return (
            self.cache._data_file(variable, station.code)
            if result == FetchResult.HAS_DATA
            else None
        )

    def _ttl(self, meta: StationMeta) -> int:
        if meta.known_data_end is None:
            return self.unknown_ttl_days
        days_ago = (dt.date.today() - dt.date.fromisoformat(meta.known_data_end)).days
        for threshold, ttl in sorted(self.ttl_schedule):
            if days_ago <= threshold:
                return ttl
        return self.ttl_schedule[-1][1]

    @staticmethod
    def _cooldown(meta: StationMeta) -> bool:
        return bool(
            meta.consecutive_errors
            and meta.last_checked
            and dt.datetime.now() - dt.datetime.fromisoformat(meta.last_checked)
            < ERROR_COOLDOWN
        )

    def _observations(self, stations: list[Station]) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for request in self.requests.values():
            codes = {
                station.code for station in select_stations(stations, request.selection)
            }
            if request.station_type is not None:
                codes &= {
                    station.code
                    for station in stations
                    if station.station_type == request.station_type_name
                }
            for code in codes:
                frame = self.cache.load_data(request.variable_name, code)
                if frame is not None:
                    frames.append(
                        frame.filter(
                            pl.col("datetime")
                            .dt.date()
                            .is_between(request.start, request.resolved_end)
                        ).with_columns(pl.lit(request.variable_name).alias("variable"))
                    )
        if not frames:
            return pl.DataFrame(
                {"station_code": [], "variable": [], "datetime": [], "value": []}
            )
        return (
            pl.concat(frames)
            .unique(subset=["station_code", "variable", "datetime"], keep="last")
            .sort("datetime")
        )


def _unique_stations(groups: Iterable[Iterable[Station]]) -> list[Station]:
    result = {station.code: station for group in groups for station in group}
    return [result[code] for code in sorted(result)]


def _stations_frame(stations: list[Station]) -> pl.DataFrame:
    return pl.DataFrame([station.__dict__ for station in stations])


def _min_date(current: str | None, value: dt.datetime | None) -> str | None:
    candidate = value.date().isoformat() if value is not None else None
    return (
        candidate
        if current is None or (candidate is not None and candidate < current)
        else current
    )


def _max_date(current: str | None, value: dt.datetime | None) -> str | None:
    candidate = value.date().isoformat() if value is not None else None
    return (
        candidate
        if current is None or (candidate is not None and candidate > current)
        else current
    )
