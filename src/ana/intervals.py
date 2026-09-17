"""Date interval algebra used by the live station cache."""

from datetime import date
from datetime import datetime
from datetime import timedelta


type CheckedRange = tuple[str, str, str]
type DateRange = tuple[str, str]

_DAY = timedelta(days=1)


def _date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _merge(pairs: list[tuple[date, date]]) -> list[tuple[date, date]]:
    if not pairs:
        return []
    ordered = sorted(pairs)
    result = [ordered[0]]
    for start, end in ordered[1:]:
        if start <= result[-1][1] + _DAY:
            result[-1] = (result[-1][0], max(result[-1][1], end))
        else:
            result.append((start, end))
    return result


def merge_checked_ranges(ranges: list[CheckedRange]) -> list[CheckedRange]:
    """Merge adjacent intervals of the same result kind."""
    by_kind: dict[str, list[tuple[date, date]]] = {}
    for start, end, kind in ranges:
        by_kind.setdefault(kind, []).append((_date(start), _date(end)))
    merged = [
        (start.isoformat(), end.isoformat(), kind)
        for kind, pairs in by_kind.items()
        for start, end in _merge(pairs)
    ]
    return sorted(merged, key=lambda item: item[0])


def find_unchecked_gaps(
    ranges: list[CheckedRange], start: str, end: str
) -> list[DateRange]:
    """Return requested dates that have never been queried."""
    request_start, request_end = _date(start), _date(end)
    covered = [
        (max(_date(left), request_start), min(_date(right), request_end))
        for left, right, _ in ranges
        if max(_date(left), request_start) <= min(_date(right), request_end)
    ]
    gaps: list[DateRange] = []
    cursor = request_start
    for covered_start, covered_end in _merge(covered):
        if cursor < covered_start:
            gaps.append((cursor.isoformat(), (covered_start - _DAY).isoformat()))
        cursor = covered_end + _DAY
    if cursor <= request_end:
        gaps.append((cursor.isoformat(), request_end.isoformat()))
    return gaps or ([(start, end)] if not covered else [])


def find_fetch_targets(
    ranges: list[CheckedRange],
    start: str,
    end: str,
    last_checked: str | None,
    ttl_days: int,
) -> list[DateRange]:
    """Return gaps plus empty intervals whose retry TTL has expired."""
    targets = find_unchecked_gaps(ranges, start, end)
    if last_checked is not None and ttl_days > 0:
        age = (datetime.now() - datetime.fromisoformat(last_checked)).days
        if age >= ttl_days:
            request_start, request_end = _date(start), _date(end)
            targets.extend(
                (
                    max(_date(left), request_start).isoformat(),
                    min(_date(right), request_end).isoformat(),
                )
                for left, right, kind in ranges
                if kind == "empty"
                and max(_date(left), request_start) <= min(_date(right), request_end)
            )
    return [
        (left.isoformat(), right.isoformat())
        for left, right in _merge(
            [(_date(start), _date(end)) for start, end in targets]
        )
    ]
