"""ANA SOAP retrieval adapter and injectable provider contract."""

import logging
import threading
import time
import xml.etree.ElementTree as ET
from calendar import monthrange
from datetime import date
from enum import StrEnum
from typing import Protocol

import polars as pl
from polars.exceptions import PolarsError
from requests import Session
from zeep import Client
from zeep import Settings
from zeep.transports import Transport

from ana.exceptions import AnaDownloadError
from ana.models import Station
from ana.parsing import parse_active_inventory
from ana.parsing import parse_conventional
from ana.parsing import parse_inventory
from ana.parsing import parse_telemetric


ANA_WSDL = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx?WSDL"
_RATE_LIMIT_SECONDS = 0.3
_VARIABLE_INFO = {
    "chuva": ("Chuva", "2"),
    "nivel": ("Nivel", "1"),
    "vazao": ("Vazao", "3"),
    "cota": ("Cota", "1"),
}
_thread_local = threading.local()
logger = logging.getLogger(__name__)


class AnaProvider(Protocol):
    """Provider used by :class:`ana.Experiment` and test fakes."""

    def fetch_inventory(self) -> list[Station]: ...

    def fetch_active_inventory(self) -> list[Station]: ...

    def fetch_series(
        self, station: Station, start: str, end: str, variable: str
    ) -> tuple["FetchResult", pl.DataFrame | None]: ...


class FetchResult(StrEnum):
    """Result constants for one station query."""

    HAS_DATA = "has_data"
    EMPTY = "empty"
    ERROR = "error"


class AnaClient:
    """SOAP adapter for ANA inventory and station series."""

    def __init__(self, timeout: int = 120, wsdl: str = ANA_WSDL) -> None:
        self.timeout = timeout
        self.wsdl = wsdl
        self._thread_local = threading.local()

    def fetch_inventory(self) -> list[Station]:
        """Fetch and parse the complete station inventory."""
        params = {
            name: ""
            for name in (
                "codEstDE",
                "codEstATE",
                "tpEst",
                "nmEst",
                "nmRio",
                "codSubBacia",
                "codBacia",
                "nmMunicipio",
                "nmEstado",
                "sgResp",
                "sgOper",
                "telemetrica",
            )
        }
        return parse_inventory(self._call("HidroInventario", **params))

    def fetch_active_inventory(self) -> list[Station]:
        """Fetch stations marked active by the telemetry service."""
        payload = self._call(
            "ListaEstacoesTelemetricas",
            statusEstacoes="0",
            origem="",
        )
        return parse_active_inventory(payload)

    def fetch_series(
        self, station: Station, start: str, end: str, variable: str
    ) -> tuple[str, pl.DataFrame | None]:
        """Fetch and parse one station series."""
        info = _VARIABLE_INFO.get(variable)
        if info is None:
            raise ValueError(f"Unknown ANA variable: {variable}")
        variable_name, data_type = info
        try:
            if station.station_type == "telemetric":
                payload = self._call(
                    "DadosHidrometeorologicos",
                    codEstacao=station.code,
                    dataInicio=start,
                    dataFim=end,
                )
                frame = parse_telemetric(payload, variable_name)
            else:
                request_start, request_end = _month_bounds(start, end)
                payload = self._call(
                    "HidroSerieHistorica",
                    codEstacao=station.code,
                    dataInicio=request_start,
                    dataFim=request_end,
                    tipoDados=data_type,
                )
                frame = parse_conventional(payload, data_type)
                if frame is not None:
                    frame = frame.filter(
                        pl.col("datetime")
                        .dt.date()
                        .is_between(date.fromisoformat(start), date.fromisoformat(end))
                    )
        except (
            AnaDownloadError,
            ET.ParseError,
            OSError,
            PolarsError,
            TimeoutError,
            ValueError,
        ):
            logger.warning("ANA query failed for station %s", station.code)
            return FetchResult.ERROR, None
        if frame is None or frame.is_empty():
            return FetchResult.EMPTY, None
        return FetchResult.HAS_DATA, frame

    def _call(self, method: str, **kwargs: str) -> bytes:
        _rate_limit()
        try:
            result = getattr(self._soap_client().service, method)(**kwargs)
            return result.content
        except Exception as error:
            raise AnaDownloadError(f"ANA SOAP call failed: {method}") from error

    def _soap_client(self) -> Client:
        client = getattr(self._thread_local, "client", None)
        if client is None:
            session = Session()
            transport = Transport(
                session=session,
                timeout=self.timeout,
                operation_timeout=self.timeout,
            )
            client = Client(
                wsdl=self.wsdl,
                settings=Settings(raw_response=True),
                transport=transport,
            )
            self._thread_local.client = client
        return client


def _rate_limit() -> None:
    last = getattr(_thread_local, "last_call", 0.0)
    remaining = _RATE_LIMIT_SECONDS - (time.monotonic() - last)
    if remaining > 0:
        time.sleep(remaining)
    _thread_local.last_call = time.monotonic()


def _month_bounds(start: str, end: str) -> tuple[str, str]:
    """Expand a conventional request to the complete calendar months."""
    start_day = date.fromisoformat(start[:10])
    end_day = date.fromisoformat(end[:10])
    first = start_day.replace(day=1)
    last = end_day.replace(day=monthrange(end_day.year, end_day.month)[1])
    return first.isoformat(), last.isoformat()
