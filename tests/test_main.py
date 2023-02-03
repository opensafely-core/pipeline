import pydantic
import pytest

from pipeline import ProjectValidationError, load_pipeline
from pipeline.models import Pipeline


def test_load_pipeline_with_file(test_file):
    data = load_pipeline(test_file)
    assert isinstance(data, Pipeline)


def test_load_pipeline_with_path(mocker, tmp_path):
    data = {
        "version": 1,
        "actions": {
            "first": {
                "run": "python:latest python foo.py",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }
    mock = mocker.patch(
        "pipeline.main.parse_yaml_file",
        auto_spec=True,
        return_value=data,
    )

    path = tmp_path / "project.yaml"

    load_pipeline(path)

    mock.assert_called_with(path, filename=None)


def test_load_pipeline_with_string(test_file):
    data = load_pipeline(test_file.read())
    assert isinstance(data, Pipeline)


def test_load_pipeline_with_yaml_error_raises_projectvalidationerror():
    """
    Test load_pipeline() raises the expected exception

    load_pipeline() reraises YAMLErrors with ProjectValidationError giving
    users a specfic ("root") exception to catch, check that is working as
    expected.
    """
    config = """
        top_level:
            duplicate: 1
            duplicate: 2
    """
    msg = 'found duplicate key "duplicate" with value "2"'
    with pytest.raises(ProjectValidationError, match=msg):
        load_pipeline(config)


def test_load_pipeline_with_project_error_raises_projectvalidationerror():
    """
    Test load_pipeline() raises the expected exception
    """
    config = """
        version: asdf
    """

    with pytest.raises(ProjectValidationError, match="Invalid project") as exc:
        load_pipeline(config)

    assert isinstance(exc.value.__cause__, pydantic.ValidationError)
