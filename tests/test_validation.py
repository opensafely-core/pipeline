import shlex

import pytest

from pipeline.exceptions import InvalidPatternError
from pipeline.validation import (
    get_output_spec_from_args,
    output_patterns_match_spec,
    validate_glob_pattern,
)


def test_validate_glob_pattern():
    validate_glob_pattern("foo/bar/*.txt", "highly_sensitive")
    validate_glob_pattern("foo/bar/*.txt", "moderately_sensitive")
    bad_patterns = [
        ("/abs/path.txt", "highly_sensitive"),
        ("not//canonical.txt", "highly_sensitive"),
        ("path/../traversal.txt", "highly_sensitive"),
        ("c:/windows/absolute.txt", "highly_sensitive"),
        ("recursive/**/glob.pattern", "highly_sensitive"),
        ("questionmark?.txt", "highly_sensitive"),
        ("/[square]brackets.txt", "highly_sensitive"),
        ("\\ftest.txt", "highly_sensitive"),
        ("metadata", "highly_sensitive"),
        ("metadata/test.txt", "highly_sensitive"),
        ("outputs/*", "highly_sensitive"),
        ("outputs/foo.*", "highly_sensitive"),
        ("outputs/output.rds", "moderately_sensitive"),
        ("outputs/output.pdf", "moderately_sensitive"),
    ]
    for pattern, sensitivity in bad_patterns:
        with pytest.raises(InvalidPatternError):
            validate_glob_pattern(pattern, sensitivity)


@pytest.mark.parametrize(
    "args,expected",
    [
        ("--output a.csv dataset.py", "a.csv"),
        ("--output=b.csv dataset.py", "b.csv"),
        ("--output='c.csv' dataset.py", "c.csv"),
        ("dataset.py --output d.csv", "d.csv"),
        ("dataset.py --output=e.csv", "e.csv"),
    ],
)
def test_get_output_spec_from_args(args, expected):
    assert get_output_spec_from_args(shlex.split(args)) == expected


@pytest.mark.parametrize(
    "expected,spec,patterns",
    [
        (True, "foo/bar.csv", ["foo/bar.csv"]),
        (False, "foo/bar.csv", ["foo/baz.csv"]),
        (True, "foo/bar:csv", ["foo/bar/*.csv"]),
        (True, "foo/bar:csv", ["foo/bar/dataset.csv", "foo/bar/events.csv"]),
        (True, "foo/bar/:csv", ["foo/bar/*.csv"]),
        (False, "foo/bar:csv", ["foo/bar/*"]),
        (False, "foo/bar:arrow", ["foo/bar/*.csv"]),
    ],
)
def test_output_patterns_match_spec(expected, spec, patterns):
    assert output_patterns_match_spec(spec, patterns) == expected
