import pathlib

import pytest


invalid_yaml = pathlib.Path(__file__).resolve().parent / "fixtures" / "invalid_yaml"
valid_yaml = pathlib.Path(__file__).resolve().parent / "fixtures" / "valid_yaml"


@pytest.fixture
def duplicate_key():
    with open(invalid_yaml / "duplicate_keys.yaml") as f:
        yield f


@pytest.fixture
def test_file():
    with open(valid_yaml / "project.yaml") as f:
        yield f
