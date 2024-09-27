from pathlib import Path

from pipeline.models import Outputs
from pipeline.outputs import get_output_dirs


def test_get_output_dirs_with_duplicates():
    outputs = Outputs.build(
        action_id="test",
        highly_sensitive={
            "a": "output/1a.csv",
            "b": "output/2a.csv",
        },
    )

    dirs = get_output_dirs(outputs)

    assert set(dirs) == {Path("output")}


def test_get_output_dirs_without_duplicates():
    outputs = Outputs.build(
        action_id="test",
        highly_sensitive={
            "a": "1a/output.csv",
            "b": "2a/output.csv",
        },
    )
    dirs = get_output_dirs(outputs)

    assert set(dirs) == {Path("1a"), Path("2a")}
