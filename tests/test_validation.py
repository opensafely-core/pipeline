import pytest

from pipeline.exceptions import InvalidPatternError
from pipeline.validation import assert_valid_glob_pattern


def test_assert_valid_glob_pattern():
    assert_valid_glob_pattern("foo/bar/*.txt")
    assert_valid_glob_pattern("foo")
    bad_patterns = [
        "/abs/path",
        "ends/in/slash/",
        "not//canonical",
        "path/../traversal",
        "c:/windows/absolute",
        "recursive/**/glob.pattern",
        "questionmark?",
        "/[square]brackets",
        "\\ftest",
        "metadata",
        "metadata/test",
    ]
    for pattern in bad_patterns:
        with pytest.raises(InvalidPatternError):
            assert_valid_glob_pattern(pattern)
