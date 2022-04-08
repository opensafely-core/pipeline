import posixpath
from pathlib import PurePosixPath, PureWindowsPath

from .exceptions import InvalidPatternError


def assert_valid_glob_pattern(pattern):
    """
    These patterns get converted into regular expressions and matched
    with a `find` command so there shouldn't be any possibility of a path
    traversal attack anyway. But it's still good to ensure that they are
    well-formed.
    """
    # Only POSIX slashes please
    if "\\" in pattern:
        raise InvalidPatternError("contains back slashes (use forward slashes only)")

    # These aren't unsafe, but they won't behave as expected so we shouldn't let
    # people use them
    for expr in ("**", "?", "["):
        if expr in pattern:
            raise InvalidPatternError(
                f"contains '{expr}' (only the * wildcard character is supported)"
            )

    if pattern.endswith("/"):
        raise InvalidPatternError(
            "looks like a directory (only files should be specified)"
        )

    # Check that the path is in normal form
    if posixpath.normpath(pattern) != pattern:
        raise InvalidPatternError(
            "is not in standard form (contains double slashes or '..' elements)"
        )

    # This is the directory we use for storing metadata about action runs and
    # we don't want outputs getting mixed up in it.
    if pattern == "metadata" or pattern.startswith("metadata/"):
        raise InvalidPatternError("should not include the metadata directory")

    # Windows has a different notion of absolute paths (e.g c:/foo) so we check
    # for both platforms
    if PurePosixPath(pattern).is_absolute() or PureWindowsPath(pattern).is_absolute():
        raise InvalidPatternError("is an absolute path")
