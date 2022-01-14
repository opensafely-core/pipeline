from pipeline import load_pipeline
from pipeline.models import Pipeline


def test_load_pipeline_with_file(test_file):
    data = load_pipeline(test_file)
    assert isinstance(data, Pipeline)


def test_load_pipeline_with_string(test_file):
    data = load_pipeline(test_file.read())
    assert isinstance(data, Pipeline)
