"""Typed requests and domain values for ANA station observations."""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from hashlib import sha256
from math import isfinite
from pathlib import Path

from ana.exceptions import AnaValidationError


class StationType(StrEnum):
    """Station categories exposed by the ANA inventory."""

    FLUVIOMETRIC = "fluviometrica"
    PLUVIOMETRIC = "pluviometrica"
    TELEMETRIC = "telemetric"


class Variable(StrEnum):
    """Hydrometeorological variables available from the service."""

    RAIN = "chuva"
    LEVEL = "nivel"
    FLOW = "vazao"
    STAGE = "cota"


class LiveHorizon(StrEnum):
    """A request horizon resolved to today's date when planned."""

    TODAY = "today"


@dataclass(frozen=True, kw_only=True)
class Station:
    """One station from the ANA inventory."""

    code: str
    name: str
    latitude: float
    longitude: float
    station_type: str
    state: str | None = None
    municipality: str | None = None
    basin: str | None = None
    subbasin: str | None = None
    river: str | None = None
    responsible: str | None = None

    def __post_init__(self) -> None:
        """Normalize the code and reject impossible coordinates."""
        code = self.code.strip()
        if not code:
            raise AnaValidationError("Station code must not be blank.")
        if not -90 <= self.latitude <= 90 or not -180 <= self.longitude <= 180:
            raise AnaValidationError(f"Invalid coordinates for station {code}.")
        object.__setattr__(self, "code", code)


@dataclass(frozen=True, kw_only=True)
class StationCodes:
    """An explicit, non-empty station selection."""

    codes: tuple[str, ...]

    def __post_init__(self) -> None:
        """Normalize and validate station codes."""
        codes = tuple(code.strip() for code in self.codes)
        if not codes or any(not code for code in codes):
            raise AnaValidationError("Station codes must not be empty.")
        if len(set(codes)) != len(codes):
            raise AnaValidationError("Station codes must not contain duplicates.")
        object.__setattr__(self, "codes", codes)


@dataclass(frozen=True, kw_only=True)
class Area:
    """A geographic bounding box used to select stations."""

    south: float
    north: float
    west: float
    east: float

    def __post_init__(self) -> None:
        """Reject non-finite or inverted bounds."""
        values = (self.south, self.north, self.west, self.east)
        if not all(isfinite(value) for value in values):
            raise AnaValidationError("Area bounds must be finite.")
        if not -90 <= self.south < self.north <= 90:
            raise AnaValidationError("Latitude bounds are invalid.")
        if not -180 <= self.west < self.east <= 180:
            raise AnaValidationError("Longitude bounds are invalid.")


@dataclass(frozen=True, kw_only=True)
class GeometryFile:
    """A vector file whose polygons select stations."""

    path: Path

    def __post_init__(self) -> None:
        """Require an existing supported vector file."""
        path = Path(self.path).expanduser().resolve()
        suffixes = {".shp", ".geojson", ".json", ".gpkg"}
        if not path.is_file():
            raise AnaValidationError(f"Geometry file does not exist: {path}")
        if path.suffix.lower() not in suffixes:
            raise AnaValidationError(f"Unsupported geometry format: {path.suffix}")
        object.__setattr__(self, "path", path)

    @property
    def digest(self) -> str:
        """Return a content digest independent of the file path."""
        return sha256(self.path.read_bytes()).hexdigest()


type StationSelection = StationCodes | Area | GeometryFile


@dataclass(frozen=True, kw_only=True)
class AnaRequest:
    """A validated request for one ANA variable and station selection."""

    start: date
    end: date | LiveHorizon
    variable: Variable | str = Variable.RAIN
    station_type: StationType | str | None = None
    selection: StationSelection | None = None

    def __post_init__(self) -> None:
        """Normalize enum inputs and validate the temporal interval."""
        if isinstance(self.variable, str):
            object.__setattr__(self, "variable", Variable(self.variable))
        if isinstance(self.station_type, str):
            object.__setattr__(self, "station_type", StationType(self.station_type))
        if not isinstance(self.start, date):
            raise AnaValidationError("start must be a date.")
        resolved_end = self.resolved_end
        if resolved_end > date.today():
            raise AnaValidationError("end cannot be later than today.")
        if self.start > resolved_end:
            raise AnaValidationError("start must not be later than end.")

    @property
    def resolved_end(self) -> date:
        """Return the fixed end or today's date for a live request."""
        return date.today() if self.end is LiveHorizon.TODAY else self.end

    @property
    def is_live(self) -> bool:
        """Whether planning follows the current date."""
        return self.end is LiveHorizon.TODAY

    @property
    def variable_name(self) -> str:
        """Return the normalized service variable name."""
        return (
            self.variable.value
            if isinstance(self.variable, Variable)
            else self.variable
        )

    @property
    def station_type_name(self) -> str | None:
        """Return the normalized station type name, if constrained."""
        if self.station_type is None:
            return None
        return (
            self.station_type.value
            if isinstance(self.station_type, StationType)
            else self.station_type
        )
