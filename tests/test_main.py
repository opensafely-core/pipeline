import pytest

from pipeline import ProjectValidationError, load_pipeline
from pipeline.models import Pipeline


def test_load_pipeline_with_file(test_file):
    data = load_pipeline(test_file)
    assert isinstance(data, Pipeline)


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
