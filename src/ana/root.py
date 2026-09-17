"""Project-root discovery for experiment defaults."""

from pathlib import Path

from ana.exceptions import AnaError


PROJECT_MARKERS = (".git", ".venv", "README.md")


class ProjectRootNotFoundError(AnaError):
    """Raised when no project-root marker can be found."""


def resolve_project_root(root_dir: Path | None = None) -> Path:
    """Resolve an explicit root or discover one from the current directory."""
    if root_dir is not None:
        root = Path(root_dir).expanduser().resolve()
        if root.exists() and not root.is_dir():
            raise NotADirectoryError(root)
        return root

    start = Path.cwd().resolve()
    ancestors = (start, *start.parents)
    for marker in PROJECT_MARKERS:
        for candidate in ancestors:
            if (candidate / marker).exists():
                return candidate
    raise ProjectRootNotFoundError(
        "Could not discover the project root; pass root_dir explicitly."
    )
