"""Executable documentation for the ANA suite.

This script is intentionally verbose because it is the shortest complete
description of the collection lifecycle. It demonstrates the same choices a
downstream study should make:

1. Select stations through a built-in :class:`ana.Region`, without copying a
   shapefile path into the consumer project.
2. Request the telemetric ``chuva`` variable, whose timestamps can represent
   the operational 12Z-to-11Z precipitation window.
3. Let :class:`ana.Experiment` discover the inventory and query only missing
   date intervals. A live request keeps one stable cache identity while the
   current tail is refreshed on later executions.
4. Materialize the observations and station catalog as a canonical Parquet
   store.

The package remains library-only. This file is a runnable example, not a
``project.scripts`` entry point and not part of the import API.

Examples
--------
Run a small state-wide collection, useful while developing a downstream map::

    uv run python scripts/experiment_guide.py --region RJ

Run the same collection for every Brazilian state::

    uv run python scripts/experiment_guide.py --region BR

Use a fixed historical window when reproducing a past report::

    uv run python scripts/experiment_guide.py \
        --region RJ --start 2026-09-15 --end 2026-09-16

The default live end is today. The dates passed to the service cover calendar
days; a report that needs one operational day should later filter the cached
timestamps to ``[yesterday 12:00, today 11:00]`` rather than assuming that a
calendar-day sum has the same meaning.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from datetime import timedelta
from pathlib import Path

from ana import AnaRequest
from ana import Experiment
from ana import LiveHorizon
from ana import Region
from ana import StationType
from ana import Variable


LOGGER = logging.getLogger("ana.experiment_guide")
REGIONS = {region.value: region for region in Region}


def build_experiment(
    *,
    region: Region,
    start: date,
    end: date | None,
    root_dir: Path,
) -> Experiment:
    """Build the documented live or historical ANA experiment.

    Parameters
    ----------
    region
        Embedded Brazil or state contour used to select stations.
    start
        First calendar day handed to the ANA service.
    end
        Fixed last calendar day, or ``None`` for the stable live horizon.
    root_dir
        Directory receiving ``.cache/ana`` and ``data/ana``.
    """
    request_end = LiveHorizon.TODAY if end is None else end
    return Experiment(
        name=f"guide-{region.value.lower()}-telemetric-rain",
        root_dir=root_dir,
        rain=AnaRequest(
            selection=region,
            start=start,
            end=request_end,
            station_type=StationType.TELEMETRIC,
            variable=Variable.RAIN,
        ),
    )


def collect(
    *,
    region: Region,
    start: date,
    end: date | None,
    root_dir: Path,
    max_workers: int,
    refresh: bool,
) -> Path:
    """Run the complete download and materialization lifecycle."""
    experiment = build_experiment(
        region=region,
        start=start,
        end=end,
        root_dir=root_dir,
    )
    paths = experiment.download(refresh=refresh, max_workers=max_workers)
    store = experiment.to_parquet()
    LOGGER.info(
        "ANA collection complete",
        extra={
            "region": region.value,
            "cached_station_files": len(paths),
            "store": str(store),
            "cache": str(experiment.cache_path),
        },
    )
    return store


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse the small set of options needed by the example."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--region",
        choices=tuple(REGIONS),
        default="RJ",
        help="Built-in region code: RJ, another UF, or BR for Brazil.",
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
    )
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--root", type=Path, default=Path())
    parser.add_argument("--max-workers", type=int, default=5)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> None:
    """Run the executable documentation example."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parsed = parse_args(args)
    if parsed.end is not None and parsed.start > parsed.end:
        raise SystemExit("--start must not be later than --end")
    collect(
        region=REGIONS[parsed.region],
        start=parsed.start,
        end=parsed.end,
        root_dir=parsed.root,
        max_workers=parsed.max_workers,
        refresh=parsed.refresh,
    )


if __name__ == "__main__":
    main()
