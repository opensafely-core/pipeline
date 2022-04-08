from pathlib import Path

from pipeline.outputs import get_output_dirs


def test_get_output_dirs_with_duplicates():
    outputs = {
        "one": {"a": "output/1a.csv"},
        "two": {"a": "output/2a.csv"},
    }

    dirs = get_output_dirs(outputs)

    assert set(dirs) == {Path("output")}


def test_get_output_dirs_without_duplicates():
    outputs = {
        "one": {"a": "1a/output.csv"},
        "two": {"a": "2a/output.csv"},
    }

    dirs = get_output_dirs(outputs)

    assert set(dirs) == {Path("1a"), Path("2a")}
