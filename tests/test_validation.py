import pytest

from pipeline.exceptions import InvalidPatternError
from pipeline.validation import assert_valid_glob_pattern


def test_assert_valid_glob_pattern():
    assert_valid_glob_pattern("foo/bar/*.txt", "highly_sensitive")
    assert_valid_glob_pattern("foo/bar/*.txt", "moderately_sensitive")
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
        ("outputs/output.rds", "moderately_sensitive"),
    ]
    for pattern, sensitivity in bad_patterns:
        with pytest.raises(InvalidPatternError):
            assert_valid_glob_pattern(pattern, sensitivity)
