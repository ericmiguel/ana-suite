"""Exception hierarchy for the ANA suite."""


class AnaError(Exception):
    """Base exception for ANA suite failures."""


class AnaValidationError(AnaError, ValueError):
    """Raised when a request or source payload is invalid."""


class AnaDownloadError(AnaError):
    """Raised when the ANA service cannot provide a response."""
