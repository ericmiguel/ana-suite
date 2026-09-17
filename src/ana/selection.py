"""Station selection helpers."""

from collections.abc import Sequence

import geopandas as gpd
from shapely.geometry import Point

from ana.contours import read_region
from ana.models import Area
from ana.models import GeometryFile
from ana.models import Region
from ana.models import Station
from ana.models import StationCodes
from ana.models import StationSelection


def select_stations(
    stations: Sequence[Station], selection: StationSelection | None
) -> list[Station]:
    """Filter an inventory by a typed station selection."""
    if selection is None:
        return list(stations)
    if isinstance(selection, StationCodes):
        wanted = set(selection.codes)
        return [station for station in stations if station.code in wanted]
    if isinstance(selection, Area):
        return [
            station
            for station in stations
            if selection.south <= station.latitude <= selection.north
            and selection.west <= station.longitude <= selection.east
        ]
    if isinstance(selection, Region):
        return _select_geometry(stations, read_region(selection))
    return _select_geometry(stations, selection)


def _select_geometry(
    stations: Sequence[Station], selection: GeometryFile | gpd.GeoDataFrame
) -> list[Station]:
    geometry = (
        gpd.read_file(selection.path)
        if isinstance(selection, GeometryFile)
        else selection
    )
    if geometry.crs is None:
        source = (
            selection.path if isinstance(selection, GeometryFile) else "embedded region"
        )
        raise ValueError(f"Geometry file has no CRS: {source}")
    points = gpd.GeoDataFrame(
        {"code": [station.code for station in stations]},
        geometry=[Point(station.longitude, station.latitude) for station in stations],
        crs="EPSG:4326",
    ).to_crs(geometry.crs)
    mask = points.geometry.within(geometry.union_all())
    codes = set(points.loc[mask, "code"])
    return [station for station in stations if station.code in codes]
