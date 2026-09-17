"""Typed, incremental access to ANA hydrometeorological station data.

The suite is library-only. It emits plain events and leaves terminal progress
and presentation to callers such as ``meteorological-reports``.
"""

from ana.cache import StationCache
from ana.cache import StationMeta
from ana.cache import experiment_cache_dir
from ana.cache import experiment_cache_key
from ana.cache import experiment_store_path
from ana.events import ItemWritten
from ana.events import PipelineEvent
from ana.events import PipelineListener
from ana.events import RequestPlanned
from ana.events import StationResolved
from ana.events import StorePlanned
from ana.exceptions import AnaDownloadError
from ana.exceptions import AnaError
from ana.exceptions import AnaValidationError
from ana.experiment import Experiment
from ana.intervals import find_fetch_targets
from ana.intervals import find_unchecked_gaps
from ana.intervals import merge_checked_ranges
from ana.models import AnaRequest
from ana.models import Area
from ana.models import GeometryFile
from ana.models import LiveHorizon
from ana.models import Station
from ana.models import StationCodes
from ana.models import StationType
from ana.models import Variable
from ana.retrieval import AnaClient
from ana.retrieval import AnaProvider
from ana.retrieval import FetchResult
from ana.root import ProjectRootNotFoundError
from ana.root import resolve_project_root


__all__ = [
    "AnaClient",
    "AnaDownloadError",
    "AnaError",
    "AnaProvider",
    "AnaRequest",
    "AnaValidationError",
    "Area",
    "Experiment",
    "FetchResult",
    "GeometryFile",
    "ItemWritten",
    "LiveHorizon",
    "PipelineEvent",
    "PipelineListener",
    "ProjectRootNotFoundError",
    "RequestPlanned",
    "Station",
    "StationCache",
    "StationCodes",
    "StationMeta",
    "StationResolved",
    "StationType",
    "StorePlanned",
    "Variable",
    "experiment_cache_dir",
    "experiment_cache_key",
    "experiment_store_path",
    "find_fetch_targets",
    "find_unchecked_gaps",
    "merge_checked_ranges",
    "resolve_project_root",
]
