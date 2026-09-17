"""Tests for request validation and cache identity."""

from datetime import date

import pytest

from ana import AnaRequest
from ana import AnaValidationError
from ana import LiveHorizon
from ana import StationCodes
from ana import Variable
from ana.cache import experiment_cache_key


def test_request_normalizes_string_variable() -> None:
    request = AnaRequest(
        start=date(2020, 1, 1),
        end=LiveHorizon.TODAY,
        variable="chuva",
    )
    assert request.variable is Variable.RAIN
    assert request.is_live


def test_station_codes_reject_duplicates() -> None:
    with pytest.raises(AnaValidationError):
        StationCodes(codes=("1", "1"))


def test_live_cache_identity_does_not_depend_on_today() -> None:
    request = AnaRequest(
        start=date(2020, 1, 1),
        end=LiveHorizon.TODAY,
        selection=StationCodes(codes=("1",)),
    )
    assert experiment_cache_key("live", {"main": request}) == experiment_cache_key(
        "live", {"main": request}
    )
