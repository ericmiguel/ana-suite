"""Tests for embedded Brazilian region resources."""

from ana import Region
from ana import Station
from ana import read_region
from ana.selection import select_stations


def test_embedded_contour_contains_all_federal_units() -> None:
    frame = read_region(Region.BRAZIL)
    assert len(frame) == 27
    assert frame.crs is not None
    assert frame.crs.to_epsg() == 4674


def test_embedded_contour_can_select_one_state() -> None:
    frame = read_region(Region.SP)
    assert len(frame) == 1
    assert frame.iloc[0]["acronym"] == "SP"


def test_region_selection_filters_inventory_points() -> None:
    stations = [
        Station(
            code="sp",
            name="Sao Paulo",
            latitude=-23.5,
            longitude=-46.6,
            station_type="pluviometrica",
        ),
        Station(
            code="am",
            name="Amazonas",
            latitude=-3.1,
            longitude=-60.0,
            station_type="pluviometrica",
        ),
    ]
    selected = select_stations(stations, Region.SP)
    assert [station.code for station in selected] == ["sp"]
