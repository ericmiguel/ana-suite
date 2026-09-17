"""Tests for the retained ANA XML parsers."""

from ana.parsing import parse_conventional


def test_parse_conventional_daily_values() -> None:
    payload = (
        b'<SerieHistorica diffgr:id="SerieHistorica1" msdata:rowOrder="0">'
        b"<DataHora>2024-03-01</DataHora>"
        b"<Chuva01>0</Chuva01><Chuva02>12,5</Chuva02>"
        b"</SerieHistorica>"
    )
    frame = parse_conventional(payload, "2")
    assert frame is not None
    assert frame["value"].to_list() == [0.0, 12.5]


def test_parse_conventional_accepts_ana_datetime() -> None:
    payload = (
        b'<SerieHistorica diffgr:id="SerieHistorica1" msdata:rowOrder="0">'
        b"<DataHora>2024-03-01 00:00:00</DataHora>"
        b"<Chuva01>4</Chuva01>"
        b"</SerieHistorica>"
    )
    frame = parse_conventional(payload, "2")
    assert frame is not None
    assert frame["datetime"].dt.date().to_list()[0].isoformat() == "2024-03-01"


def test_parse_telemetric_strips_publisher_datetime_padding() -> None:
    from ana.parsing import parse_telemetric

    payload = (
        b"<root><DocumentElement>"
        b"<row><DataHora>2026-09-16 00:00:00 </DataHora>"
        b"<Chuva>1.5</Chuva></row>"
        b"</DocumentElement></root>"
    )
    frame = parse_telemetric(payload, "Chuva")
    assert frame is not None
    assert frame["value"].to_list() == [1.5]


def test_parse_active_telemetry_inventory() -> None:
    from ana.parsing import parse_active_inventory

    payload = (
        b'<Table diffgr:id="Table1" msdata:rowOrder="0">'
        b"<NomeEstacao>ACTIVE</NomeEstacao><CodEstacao>00047008</CodEstacao>"
        b"<Latitude>-0.7</Latitude><Longitude>-47.8</Longitude>"
        b"<Bacia>3</Bacia><SubBacia>32</SubBacia>"
        b"</Table>"
    )
    stations = parse_active_inventory(payload)
    assert stations[0].code == "00047008"
    assert stations[0].station_type == "telemetric"
