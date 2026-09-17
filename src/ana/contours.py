"""Embedded Brazilian UF contours used by named station selections."""

from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import as_file
from importlib.resources import files
from pathlib import Path

import geopandas as gpd

from ana.models import Region


_CONTOUR_RESOURCE = files("ana").joinpath("data", "shapes")


@contextmanager
def region_file() -> Iterator[Path]:
    """Yield a filesystem path for the embedded UF contour resource."""
    with as_file(_CONTOUR_RESOURCE) as path:
        yield path / "uf.shp"


def read_region(region: Region) -> gpd.GeoDataFrame:
    """Read the embedded contour for Brazil or one Brazilian state."""
    with region_file() as path:
        frame = gpd.read_file(path)
    if region is Region.BRAZIL:
        return frame
    return frame.loc[frame["acronym"] == region.value].copy()
