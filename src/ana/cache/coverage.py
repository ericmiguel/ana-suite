"""Coverage summaries for ANA stores.

ANA fragments are station series, so the spatial axis is a station set rather
than a grid: the coverage axes that matter are time, variables, and stations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ana.models import AnaRequest
from ana.models import Area
from ana.models import GeometryFile
from ana.models import Region
from ana.models import StationCodes


if TYPE_CHECKING:
    from collections.abc import Mapping


GRID_SIGNATURE = "point:stations"


def summarize_coverage(requests: Mapping[str, object]) -> dict[str, object]:
    """Return the time, variable, and station coverage of a request set."""
    starts: list[str] = []
    ends: list[str] = []
    variables: dict[str, None] = {}
    stations: dict[str, None] = {}
    for request in requests.values():
        if not isinstance(request, AnaRequest):
            continue
        starts.append(request.start.isoformat())
        ends.append(request.resolved_end.isoformat())
        variables.setdefault(request.variable_name, None)
        for selector in _station_selectors(request):
            stations.setdefault(selector, None)
    return {
        "time": [min(starts), max(ends)] if starts else [],
        "variables": sorted(variables),
        "stations": sorted(stations),
        "grid": GRID_SIGNATURE,
    }


def _station_selectors(request: AnaRequest) -> list[str]:
    """Describe the station set a request selects on a point axis."""
    selection = request.selection
    if selection is None:
        return ["all"]
    if isinstance(selection, StationCodes):
        return list(selection.codes)
    if isinstance(selection, Region):
        return [f"region:{selection.value}"]
    if isinstance(selection, Area):
        return [
            "area:"
            f"{selection.south},{selection.north},{selection.west},{selection.east}"
        ]
    if isinstance(selection, GeometryFile):
        return [f"geometry:{selection.digest}"]
    return ["all"]
