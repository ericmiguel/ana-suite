"""Tests for live-cache date algebra."""

from datetime import datetime
from datetime import timedelta

from ana.intervals import find_fetch_targets
from ana.intervals import find_unchecked_gaps
from ana.intervals import merge_checked_ranges


def test_merge_checked_ranges_keeps_result_kinds_separate() -> None:
    ranges = [
        ("2024-01-01", "2024-01-31", "has_data"),
        ("2024-02-01", "2024-02-02", "has_data"),
        ("2024-01-01", "2024-01-31", "empty"),
    ]
    assert merge_checked_ranges(ranges) == [
        ("2024-01-01", "2024-02-02", "has_data"),
        ("2024-01-01", "2024-01-31", "empty"),
    ]


def test_find_unchecked_gaps_returns_only_missing_spans() -> None:
    ranges = [("2024-01-01", "2024-01-05", "has_data")]
    assert find_unchecked_gaps(ranges, "2024-01-01", "2024-01-10") == [
        ("2024-01-06", "2024-01-10")
    ]


def test_expired_empty_interval_is_rechecked() -> None:
    old = (datetime.now() - timedelta(days=10)).isoformat()
    ranges = [("2024-01-01", "2024-01-05", "empty")]
    assert find_fetch_targets(ranges, "2024-01-01", "2024-01-05", old, 7) == [
        ("2024-01-01", "2024-01-05")
    ]
