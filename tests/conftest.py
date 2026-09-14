import pathlib

import pytest

from pipeline.features import LATEST_VERSION, MINIMUM_VERSION


invalid_yaml = pathlib.Path(__file__).resolve().parent / "fixtures" / "invalid_yaml"
valid_yaml = pathlib.Path(__file__).resolve().parent / "fixtures" / "valid_yaml"


@pytest.fixture
def duplicate_key():
    with open(invalid_yaml / "duplicate_keys.yaml") as f:
        yield f


@pytest.fixture
def test_file(tmp_path, version):
    file_text = (valid_yaml / "project.yaml").read_text()
    file_text = file_text.replace("<VERSION>", str(version))
    (tmp_path / "project.yaml").write_text(file_text)

    with open(tmp_path / "project.yaml") as f:
        yield f


@pytest.fixture(params=list(range(MINIMUM_VERSION, LATEST_VERSION + 1)))
def version(request):
    return request.param
