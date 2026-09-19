"""Provenance ranks for ANA fragments.

ANA is the single provider for these station series, so every fragment carries
the same rank and no fragment supersedes another.
"""

from __future__ import annotations


SINGLE = 0

LEGEND: dict[int, str] = {
    SINGLE: "single",
}


def legend_payload() -> dict[str, str]:
    """Return the manifest-ready provenance legend."""
    return {str(code): label for code, label in LEGEND.items()}
