"""Atomic Parquet materialization for ANA experiments."""

import json
import shutil
from pathlib import Path

import polars as pl


STATIONS_TABLE = "stations"
OBSERVATIONS_TABLE = "observations"
MANIFEST_NAME = "manifest.json"


def write_store(
    destination: Path,
    stations: pl.DataFrame,
    observations: pl.DataFrame,
    *,
    overwrite: bool,
) -> Path:
    """Write both tables into a temporary directory and install atomically."""
    destination = Path(destination)
    if destination.exists() and not overwrite:
        raise FileExistsError(f"Store already exists: {destination}")
    temporary = destination.with_name(f".{destination.name}.part")
    remove_store(temporary)
    temporary.mkdir(parents=True)
    stations.write_parquet(temporary / f"{STATIONS_TABLE}.parquet")
    _write_observation_partitions(temporary, observations)
    (temporary / MANIFEST_NAME).write_text(
        json.dumps(
            {
                "schema": 1,
                "source": "ana",
                "tables": [STATIONS_TABLE, OBSERVATIONS_TABLE],
            },
            indent=2,
        )
        + "\n"
    )
    if destination.exists():
        remove_store(destination)
    temporary.replace(destination)
    return destination


def read_table(
    store: Path,
    table: str,
    *,
    station_code: str | None = None,
    variable: str | None = None,
) -> pl.DataFrame:
    """Read a canonical table, optionally filtering observations."""
    store = Path(store)
    if table == STATIONS_TABLE:
        return pl.read_parquet(store / "stations.parquet")
    if table != OBSERVATIONS_TABLE:
        raise ValueError(f"Unknown ANA table: {table}")
    paths = sorted(store.glob("observations/**/*.parquet"))
    if not paths:
        return pl.DataFrame()
    frame = pl.read_parquet(paths)
    if station_code is not None:
        frame = frame.filter(pl.col("station_code") == station_code)
    if variable is not None:
        frame = frame.filter(pl.col("variable") == variable)
    return frame.sort(["station_code", "variable", "datetime"])


def remove_store(path: Path) -> None:
    """Remove a store directory or an incomplete temporary path."""
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _write_observation_partitions(store: Path, observations: pl.DataFrame) -> None:
    if observations.is_empty():
        return
    for keys, frame in observations.partition_by(
        ["variable", "station_code"], as_dict=True
    ).items():
        variable, station_code = keys
        path = (
            store
            / "observations"
            / f"variable={variable}"
            / f"station={station_code}.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
