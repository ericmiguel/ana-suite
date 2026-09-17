"""Tests for ANA service request normalization."""

from ana.retrieval import _month_bounds


def test_conventional_request_expands_to_calendar_months() -> None:
    assert _month_bounds("2026-04-29", "2026-04-30") == (
        "2026-04-01",
        "2026-04-30",
    )
