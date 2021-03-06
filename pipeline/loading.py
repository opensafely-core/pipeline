from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from ruamel.yaml.error import (
    MarkedYAMLError,
    MarkedYAMLFutureWarning,
    MarkedYAMLWarning,
    YAMLError,
    YAMLFutureWarning,
    YAMLStreamError,
    YAMLWarning,
)
from ruamel.yaml.main import YAML

from . import exceptions


# We use "safe" to avoid malicious inputs, and "rt" (round-trip) in order to
# have detailed line number information to help generate better error message
# We also use the pure-Python version here as we don't care about speed and it
# gives better error messages (and consistent behaviour cross-platform)
#
# We're ignoring arg-type here because the type specification for the YAML
# class is incorrect for this variable.  It's defined as Optional[Text] [1] but
# the implementation accepts a list or string [2].
#
# 1: https://sourceforge.net/p/ruamel-yaml/code/ci/0.17.20/tree/main.py#l55
# 2: https://sourceforge.net/p/ruamel-yaml/code/ci/0.17.20/tree/main.py#l66
PARSER = YAML(typ=["safe", "rt"], pure=True)  # type: ignore[arg-type]


RuamelMarkedError = Union[MarkedYAMLError, MarkedYAMLFutureWarning, MarkedYAMLWarning]


def make_yaml_error_more_helpful(
    exc: RuamelMarkedError, name: str | None
) -> RuamelMarkedError:
    """
    Improve the error message we expose when encountering errors.

    ruamel produces quite helpful error messages but they refer to the file as
    `<byte_string>` (which will be confusing for users) and they also include
    notes and warnings to developers about API changes. This function attempts
    to fix these issues, but just returns the exception unchanged if the type
    of exception we get doesn't have expected attributes.
    """
    exc.note = None

    if name:
        exc.context_mark.name = name
        exc.problem_mark.name = name

    return exc


def parse_yaml_file(data: str | Path, filename: str | None = None) -> dict[str, Any]:
    try:
        return PARSER.load(data)  # type: ignore[no-any-return]
    # ruamel doesn't have a nice exception hierarchy so we have to catch these
    # four separate base classes
    except (
        YAMLError,
        YAMLStreamError,
        YAMLWarning,
        YAMLFutureWarning,
    ) as exc:
        marked_error_classes = (
            MarkedYAMLError,
            MarkedYAMLWarning,
            MarkedYAMLFutureWarning,
        )
        if isinstance(exc, marked_error_classes):
            # the attributes we want to improve are only on the Marked*
            # exception classes, but we want to avoid their parent classes,
            # hence not using isinstance here.
            exc = make_yaml_error_more_helpful(exc, filename)
        # wrap in our error
        raise exceptions.YAMLError(str(exc)) from exc
