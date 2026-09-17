"""Parsers for ANA SOAP XML payloads."""

import re
import xml.etree.ElementTree as ET

import polars as pl

from ana.models import Station


_TABLE = re.compile(
    r'<Table diffgr:id="Table[0-9]+" msdata:rowOrder="[0-9]+">(.*?)</Table>',
    re.DOTALL,
)
_SERIES = re.compile(
    r'<SerieHistorica diffgr:id="SerieHistorica[0-9]+" '
    r'msdata:rowOrder="[0-9]+">(.*?)</SerieHistorica>',
    re.DOTALL,
)
_TAG = re.compile(r"<([a-zA-Z0-9]+)>(.*?)</[a-zA-Z0-9]+>")
_TYPE_MAP = {"1": "fluviometrica", "2": "pluviometrica"}
_VARIABLE_MAP = {"1": "Cota", "2": "Chuva", "3": "Vazao"}


def parse_inventory(payload: bytes) -> list[Station]:
    """Parse the ANA inventory response."""
    stations: list[Station] = []
    for table in _TABLE.findall(payload.decode("utf-8")):
        row = dict(_TAG.findall(table))
        code = row.get("Codigo", "").strip()
        if not code:
            continue
        telemetric = row.get("TipoEstacaoTelemetrica", "0") == "1"
        station_type = (
            "telemetric"
            if telemetric
            else _TYPE_MAP.get(row.get("TipoEstacao", "2"), "unknown")
        )
        if station_type == "unknown":
            continue
        stations.append(
            Station(
                code=code,
                name=row.get("Nome", ""),
                latitude=_number(row.get("Latitude", "0")),
                longitude=_number(row.get("Longitude", "0")),
                station_type=station_type,
                state=row.get("nmEstado"),
                municipality=row.get("nmMunicipio"),
                basin=row.get("BaciaCodigo"),
                subbasin=row.get("SubBaciaCodigo"),
                river=row.get("RioNome"),
                responsible=row.get("ResponsavelSigla"),
            )
        )
    return stations


def parse_conventional(payload: bytes, data_type: str) -> pl.DataFrame | None:
    """Parse a daily ``HidroSerieHistorica`` response."""
    rows = [
        dict(_TAG.findall(table)) for table in _SERIES.findall(payload.decode("utf-8"))
    ]
    if not rows:
        return None
    variable = _VARIABLE_MAP.get(data_type, "Chuva")
    columns = [
        column
        for column in rows[0]
        if column.startswith(variable) and not column.endswith("Status")
    ]
    if not columns or "DataHora" not in rows[0]:
        return None
    records: list[dict[str, object]] = []
    for row in rows:
        base = row["DataHora"]
        for column in columns:
            day = int(column.removeprefix(variable))
            records.append(
                {
                    "datetime": f"{base}T00:00:00",
                    "day": day,
                    "value": _number_or_none(row.get(column)),
                }
            )
    return (
        pl.DataFrame(records)
        .with_columns(
            (
                pl.col("datetime").str.to_datetime(strict=False)
                + pl.duration(days=pl.col("day") - 1)
            ).alias("datetime")
        )
        .select("datetime", "value")
        .drop_nulls("value")
        .unique(subset=["datetime"], keep="last")
        .sort("datetime")
    )


def parse_telemetric(payload: bytes, variable: str) -> pl.DataFrame | None:
    """Parse one variable from a ``DadosHidrometeorologicos`` response."""
    root = ET.fromstring(payload.decode("utf-8"))
    records = []
    for element in root.findall(".//DocumentElement/*"):
        row = {child.tag: child.text for child in element}
        if "DataHora" in row and variable in row:
            records.append(
                {
                    "datetime": row["DataHora"],
                    "value": _number_or_none(row[variable]),
                }
            )
    if not records:
        return None
    return (
        pl.DataFrame(records)
        .with_columns(pl.col("datetime").str.to_datetime(strict=False))
        .drop_nulls("value")
        .unique(subset=["datetime"], keep="last")
        .sort("datetime")
    )


def _number(value: str) -> float:
    return float(value.replace(",", "."))


def _number_or_none(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        return _number(value)
    except ValueError:
        return None
