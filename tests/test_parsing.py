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
